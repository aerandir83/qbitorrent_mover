import logging
import os
import shutil
import subprocess
from pathlib import Path
import tempfile
import getpass
import time
import shlex
import fcntl
import atexit
import errno
import typing
from typing import Optional, Dict, List, Tuple, Any, Set, TYPE_CHECKING
import configparser

from .ssh_manager import SSHConnectionPool
from .transfer_manager import Timeouts
from rich.logging import RichHandler
from rich.console import Console

if TYPE_CHECKING:
    import qbittorrentapi

class LockFile:
    """Atomic lock file using fcntl (Unix only)."""

    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self.lock_fd: Optional[int] = None
        self._acquired = False

    def acquire(self) -> None:
        """Acquire the lock. Raises RuntimeError if already locked."""
        try:
            # Open the file, creating it if it doesn't exist
            self.lock_fd = open(self.lock_path, 'w')
            # Try to acquire an exclusive, non-blocking lock
            fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Write the current PID to the lock file
            self.lock_fd.write(str(os.getpid()))
            self.lock_fd.flush()
            # Register the release method to be called on exit
            atexit.register(self.release)
            self._acquired = True
        except (IOError, BlockingIOError):
            # If locking fails, close the file descriptor if it was opened
            if self.lock_fd:
                self.lock_fd.close()
            # Announce that the script is already running
            pid = self.get_locking_pid()
            if pid:
                raise RuntimeError(f"Script is already running with PID {pid} (lock file: {self.lock_path})")
            else:
                raise RuntimeError(f"Script is already running (lock file: {self.lock_path})")

    def release(self) -> None:
        """Release the lock."""
        if self.lock_fd and self._acquired:
            try:
                # Release the lock
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
                self.lock_fd.close()
                self.lock_path.unlink(missing_ok=True)
                self._acquired = False
            except Exception as e:
                logging.error(f"Error releasing lock file '{self.lock_path}': {e}")
            finally:
                self.lock_fd = None

    def get_locking_pid(self) -> Optional[str]:
        """Read the PID from the lock file, if it exists."""
        if self.lock_path.exists():
            try:
                return self.lock_path.read_text().strip()
            except IOError:
                return None
        return None

# --- System & Health Check Functions ---

def cleanup_orphaned_cache(destination_qbit: "qbittorrentapi.Client") -> None:
    """
    Scans for leftover cache directories from previous runs and removes them
    if the corresponding torrent is already successfully on the destination client.
    """
    temp_dir = Path(tempfile.gettempdir())
    cache_prefix = "torrent_mover_cache_"
    logging.info("--- Running Orphaned Cache Cleanup ---")
    try:
        all_dest_hashes: Set[str] = {t.hash for t in destination_qbit.torrents_info() if t.progress == 1}
        logging.info(f"Found {len(all_dest_hashes)} completed torrent hashes on the destination client for cleanup check.")
    except Exception as e:
        logging.error(f"Could not retrieve torrents from destination client for cache cleanup: {e}")
        return
    found_cache_dirs = 0
    cleaned_cache_dirs = 0
    for item in temp_dir.iterdir():
        if item.is_dir() and item.name.startswith(cache_prefix):
            found_cache_dirs += 1
            torrent_hash = item.name[len(cache_prefix):]
            if torrent_hash in all_dest_hashes:
                logging.warning(f"Found orphaned cache for a completed torrent: {torrent_hash}. Removing.")
                try:
                    shutil.rmtree(item)
                    cleaned_cache_dirs += 1
                except OSError as e:
                    logging.error(f"Failed to remove orphaned cache directory '{item}': {e}")
            else:
                logging.info(f"Found valid cache for an incomplete or non-existent torrent: {torrent_hash}. Leaving it for resume.")
    if found_cache_dirs == 0:
        logging.info("No leftover cache directories found.")
    else:
        logging.info(f"Cleanup complete. Removed {cleaned_cache_dirs}/{found_cache_dirs} orphaned cache directories.")

def recover_cached_torrents(source_qbit: "qbittorrentapi.Client", destination_qbit: "qbittorrentapi.Client") -> List["qbittorrentapi.TorrentDictionary"]:
    """
    Scans for leftover cache directories from previous runs and adds them back
    to the processing queue if the torrent is not on the destination.
    """
    temp_dir = Path(tempfile.gettempdir())
    cache_prefix = "torrent_mover_cache_"
    logging.info("--- Checking for incomplete cached transfers to recover ---")
    try:
        all_dest_hashes: Set[str] = {t.hash for t in destination_qbit.torrents_info()}
        logging.info(f"Found {len(all_dest_hashes)} torrent hashes on the destination client for recovery check.")
    except Exception as e:
        logging.error(f"Could not retrieve torrents from destination client for cache recovery: {e}")
        return []
    cache_dirs = [item for item in temp_dir.iterdir() if item.is_dir() and item.name.startswith(cache_prefix)]
    if not cache_dirs:
        logging.info("No leftover cache directories found to recover.")
        return []
    logging.info(f"Found {len(cache_dirs)} cache directories to check for recovery.")
    hashes_to_recover = []
    for item in cache_dirs:
        torrent_hash = item.name[len(cache_prefix):]
        if torrent_hash not in all_dest_hashes:
            logging.warning(f"Found an incomplete cached transfer: {torrent_hash}. Will attempt to recover.")
            hashes_to_recover.append(torrent_hash)
    if not hashes_to_recover:
        logging.info("All found cache directories correspond to completed torrents.")
        return []
    try:
        source_torrents = source_qbit.torrents_info(torrent_hashes=hashes_to_recover)
        recovered_torrents = list(source_torrents)
        if recovered_torrents:
            logging.info(f"Successfully recovered {len(recovered_torrents)} torrent(s) from cache. They will be added to the queue.")
        else:
            logging.warning("Could not find matching torrents on the source for the incomplete cache directories.")
        return recovered_torrents
    except Exception as e:
        logging.error(f"An error occurred while trying to get torrent info from the source for recovery: {e}")
        return []

def destination_health_check(config: configparser.ConfigParser, total_transfer_size_bytes: int, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> bool:
    """
    Performs checks on the destination (local or remote) to ensure it's ready.
    """
    logging.info("--- Running Destination Health Check ---")
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    dest_path = config['DESTINATION_PATHS'].get('destination_path')
    remote_config = config['DESTINATION_SERVER'] if transfer_mode == 'sftp_upload' and 'DESTINATION_SERVER' in config else None
    dest_pool = ssh_connection_pools.get('DESTINATION_SERVER') if remote_config else None
    available_space = -1
    GB_BYTES = 1024**3
    try:
        if remote_config and dest_pool:
            with dest_pool.get_connection() as (sftp, ssh):
                command = f"df -kP {shlex.quote(dest_path)}"
                stdin, stdout, stderr = ssh.exec_command(command, timeout=Timeouts.SSH_EXEC)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    stderr_output = stderr.read().decode('utf-8').strip()
                    if "not found" in stderr_output or "no such" in stderr_output.lower():
                        raise FileNotFoundError(f"The 'df' command was not found on the remote server. Cannot check disk space.")
                    raise Exception(f"'df' command failed with exit code {exit_status}. Stderr: {stderr_output}")
                output = stdout.read().decode('utf-8').strip().splitlines()
                if len(output) < 2:
                    stderr_output = stderr.read().decode('utf-8').strip()
                    if "no such file or directory" in stderr_output.lower():
                        raise FileNotFoundError(f"The remote path '{dest_path}' does not exist.")
                    raise ValueError(f"Could not parse 'df' output. Raw output: '{' '.join(output)}'")
                parts = output[1].split()
                if len(parts) < 4:
                    raise ValueError(f"Unexpected 'df' output format. Line: '{output[1]}'")
                available_kb = int(parts[3])
                available_space = available_kb * 1024
        else:
            stats = os.statvfs(dest_path)
            available_space = stats.f_bavail * stats.f_frsize
        required_space_gb = total_transfer_size_bytes / GB_BYTES
        available_space_gb = available_space / GB_BYTES
        logging.info(f"Required space for this run: {required_space_gb:.2f} GB. Available on destination: {available_space_gb:.2f} GB.")
        if total_transfer_size_bytes > available_space:
            logging.error("FATAL: Not enough disk space on the destination.")
            logging.error(f"Required: {required_space_gb:.2f} GB, Available: {available_space_gb:.2f} GB.")
            return False
        else:
            logging.info("[green]SUCCESS:[/] Destination has enough disk space.")
    except FileNotFoundError:
        logging.error(f"FATAL: The destination path '{dest_path}' does not exist.")
        if not remote_config:
            logging.error("Please ensure the base directory is created and accessible on the local filesystem.")
        else:
            logging.error("Please ensure the base directory is created and accessible on the remote server.")
        return False
    except Exception as e:
        logging.error(f"An error occurred during disk space check: {e}", exc_info=True)
        return False
    if not test_path_permissions(dest_path, remote_config=remote_config, ssh_connection_pools=ssh_connection_pools):
        logging.error("FATAL: Destination permission check failed.")
        return False
    logging.info("--- Destination Health Check Passed ---")
    return True

def change_ownership(path_to_change: str, user: str, group: str, remote_config: Optional[configparser.SectionProxy] = None, dry_run: bool = False, ssh_connection_pools: Optional[Dict[str, SSHConnectionPool]] = None) -> None:
    """
    Changes the ownership of a file or directory, either locally or remotely.
    """
    if not user and not group:
        return
    owner_spec = f"{user or ''}:{group or ''}".strip(':')
    if dry_run:
        logging.info(f"[DRY RUN] Would change ownership of '{path_to_change}' to '{owner_spec}'.")
        return
    if remote_config and ssh_connection_pools:
        logging.info(f"Attempting to change remote ownership of '{path_to_change}' to '{owner_spec}'...")
        pool = ssh_connection_pools.get('DESTINATION_SERVER')
        if not pool:
            logging.error("Could not find SSH pool for DESTINATION_SERVER.")
            return
        try:
            with pool.get_connection() as (sftp, ssh):
                remote_command = f"chown -R -- {shlex.quote(owner_spec)} {shlex.quote(path_to_change)}"
                logging.debug(f"Executing remote command: {remote_command}")
                stdin, stdout, stderr = ssh.exec_command(remote_command, timeout=Timeouts.SSH_EXEC)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status == 0:
                    logging.info("Remote ownership changed successfully.")
                else:
                    stderr_output = stderr.read().decode('utf-8').strip()
                    logging.error(f"Failed to change remote ownership for '{path_to_change}'. Exit code: {exit_status}, Stderr: {stderr_output}")
        except Exception as e:
            logging.error(f"An exception occurred during remote chown: {e}", exc_info=True)
    else:
        logging.info(f"Attempting to change local ownership of '{path_to_change}' to '{owner_spec}'...")
        try:
            if shutil.which("chown") is None:
                logging.warning("'chown' command not found locally. Skipping ownership change.")
                return
            command = ["chown", "-R", owner_spec, path_to_change]
            process = subprocess.run(command, capture_output=True, text=True, check=False)
            if process.returncode == 0:
                logging.info("Local ownership changed successfully.")
            else:
                logging.error(f"Failed to change local ownership for '{path_to_change}'. Exit code: {process.returncode}, Stderr: {process.stderr.strip()}")
        except Exception as e:
            logging.error(f"An exception occurred during local chown: {e}", exc_info=True)

def _delete_destination_content(dest_content_path: str, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> None:
    """Deletes the content from the destination path, either locally or remotely."""
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    is_remote = (transfer_mode == 'sftp_upload')

    logging.warning(f"Attempting to delete destination content: {dest_content_path}")
    try:
        if is_remote:
            # Remote deletion
            dest_pool = ssh_connection_pools.get('DESTINATION_SERVER')
            if not dest_pool:
                logging.error("Cannot delete remote content: Destination SSH pool not found.")
                return

            with dest_pool.get_connection() as (sftp, ssh):
                # Check if it's a directory or file
                try:
                    stat = sftp.stat(dest_content_path)
                    if stat.st_mode & 0o40000: # S_ISDIR
                        logging.debug("Destination is a directory. Using 'rm -rf'.")
                        command = f"rm -rf {shlex.quote(dest_content_path)}"
                        stdin, stdout, stderr = ssh.exec_command(command, timeout=Timeouts.SSH_EXEC)
                        exit_status = stdout.channel.recv_exit_status()
                        if exit_status != 0:
                            logging.error(f"Failed to delete remote directory: {stderr.read().decode()}")
                    else:
                        logging.debug("Destination is a file. Using 'sftp.remove'.")
                        sftp.remove(dest_content_path)
                except FileNotFoundError:
                    logging.warning(f"Destination content not found (already deleted?): {dest_content_path}")
        else:
            # Local deletion
            p = Path(dest_content_path)
            if p.is_dir():
                logging.debug("Destination is a directory. Using 'shutil.rmtree'.")
                shutil.rmtree(p)
            elif p.is_file():
                logging.debug("Destination is a file. Using 'os.remove'.")
                p.unlink()
            else:
                logging.warning(f"Destination content not found (already deleted?): {dest_content_path}")

        logging.info(f"Successfully deleted destination content: {dest_content_path}")

    except Exception as e:
        logging.error(f"An error occurred while deleting destination content: {e}", exc_info=True)

def setup_logging(script_dir: Path, dry_run: bool, test_run: bool, debug: bool) -> None:
    """Configures logging to both console and a file."""
    log_dir = script_dir / 'logs'
    log_dir.mkdir(exist_ok=True)
    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    log_file_name = f"torrent_mover_{timestamp}.log"
    log_file_path = log_dir / log_file_name
    logger = logging.getLogger()
    log_level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(log_level)
    if logger.hasHandlers():
        logger.handlers.clear()
    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    rich_handler = RichHandler(level=log_level, show_path=False, rich_tracebacks=True, markup=True, console=Console(stderr=True))
    rich_formatter = logging.Formatter('%(message)s')
    rich_handler.setFormatter(rich_formatter)
    logger.addHandler(rich_handler)
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    logging.info("--- Torrent Mover script started ---")
    if dry_run:
        logging.warning("!!! DRY RUN MODE ENABLED. NO CHANGES WILL BE MADE. !!!")
    if test_run:
        logging.warning("!!! TEST RUN MODE ENABLED. SOURCE TORRENTS WILL NOT BE DELETED. !!!")

def pid_exists(pid: int) -> bool:
    """Check whether a process with the given PID exists."""
    if pid < 0: return False
    if pid == 0: return False
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH: return False
        elif err.errno == errno.EPERM: return True
        else: raise
    else:
        return True

def test_path_permissions(path_to_test: str, remote_config: Optional[configparser.SectionProxy] = None, ssh_connection_pools: Optional[Dict[str, SSHConnectionPool]] = None) -> bool:
    """
    Tests write permissions for a given path by creating and deleting a temporary file.
    """
    if remote_config:
        logging.info(f"--- Running REMOTE Permission Test on: {path_to_test} ---")
        if not ssh_connection_pools:
            logging.error("SSH connection pools not available for remote permission test.")
            return False
        pool = ssh_connection_pools.get('DESTINATION_SERVER')
        if not pool:
            logging.error("Could not find SSH pool for DESTINATION_SERVER.")
            return False
        try:
            with pool.get_connection() as (sftp, ssh):
                path_to_test = path_to_test.replace('\\', '/')
                test_file_path = f"{path_to_test.rstrip('/')}/permission_test_{os.getpid()}.tmp"
                logging.info(f"Attempting to create a remote test file: {test_file_path}")
                with sftp.open(test_file_path, 'w') as f:
                    f.write('test')
                logging.info("Remote test file created successfully.")
                sftp.remove(test_file_path)
            logging.info("Remote test file removed successfully.")
            logging.info(f"[bold green]SUCCESS:[/] Remote write permissions are correctly configured for '{remote_config['username']}' on path '{path_to_test}'")
            return True
        except PermissionError:
            logging.error(f"[bold red]FAILURE:[/] Permission denied on remote server when trying to write to '{path_to_test}'.\n"
                          f"Please ensure the user '{remote_config['username']}' has write access to this path on the remote server.")
            return False
        except Exception as e:
            logging.error(f"[bold red]FAILURE:[/] An unexpected error occurred during remote permission test: {e}", exc_info=True)
            return False
    else:
        p = Path(path_to_test)
        logging.info(f"--- Running LOCAL Permission Test on: {p} ---")
        if not p.exists():
            logging.error(f"Error: The local path '{p}' does not exist.\nPlease ensure the base directory is created and accessible.")
            return False
        if not p.is_dir():
            logging.error(f"Error: The local path '{p}' is not a directory.")
            return False
        test_file_path = p / f"permission_test_{os.getpid()}.tmp"
        try:
            logging.info(f"Attempting to create a test file: {test_file_path}")
            with open(test_file_path, 'w') as f:
                f.write('test')
            logging.info("Test file created successfully.")
            os.remove(test_file_path)
            logging.info("Test file removed successfully.")
            logging.info(f"[bold green]SUCCESS:[/] Local write permissions are correctly configured for {p}")
            return True
        except PermissionError:
            logging.error(f"[bold red]FAILURE:[/] Permission denied when trying to write to local path '{p}'.\n"
                          f"Please ensure the user '{getpass.getuser()}' running the script has write access to this path.")
            return False
        except Exception as e:
            logging.error(f"[bold red]FAILURE:[/] An unexpected error occurred during local permission test: {e}", exc_info=True)
            return False
