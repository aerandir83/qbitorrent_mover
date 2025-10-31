"""Manages system-level tasks and interactions.

This module provides functionalities for process locking, logging setup, system
health checks (disk space, permissions), cache management, and file ownership
changes. It acts as the interface between the application and the underlying
operating system for tasks that are not specific to torrents or file transfers.
"""
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
import paramiko
from . import ssh_manager
import sys

from .ssh_manager import SSHConnectionPool
from .transfer_manager import Timeouts
from rich.logging import RichHandler
from rich.console import Console

if TYPE_CHECKING:
    import qbittorrentapi

class LockFile:
    """An atomic lock file implementation using `fcntl` for Unix-based systems.

    This class ensures that only one instance of the script can run at a time
    by creating and locking a file. The lock is automatically released when the
    script exits gracefully.

    Attributes:
        lock_path: The `Path` object representing the lock file's location.
        lock_fd: The file descriptor for the opened lock file.
    """

    def __init__(self, lock_path: Path):
        """Initializes the LockFile instance.

        Args:
            lock_path: The path where the lock file should be created.
        """
        self.lock_path = lock_path
        self.lock_fd: Optional[int] = None
        self._acquired = False

    def acquire(self) -> None:
        """Acquires an exclusive, non-blocking lock on the lock file.

        If the lock is already held by another process, it reads the PID from
        the lock file and raises a `RuntimeError`.

        Raises:
            RuntimeError: If the script is already running and holds the lock.
        """
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
        """Releases the lock and deletes the lock file."""
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
        """Reads the PID of the process holding the lock from the lock file.

        Returns:
            The PID as a string if the lock file exists and is readable,
            otherwise None.
        """
        if self.lock_path.exists():
            try:
                return self.lock_path.read_text().strip()
            except IOError:
                return None
        return None

# --- System & Health Check Functions ---

def cleanup_orphaned_cache(destination_qbit: "qbittorrentapi.Client") -> None:
    """Removes leftover cache directories from previous runs.

    This function scans the system's temporary directory for cache folders created
    by this script. It checks if the torrent hash associated with a cache folder
    corresponds to a torrent that has been successfully completed on the
    destination client. If so, the cache is considered orphaned and is deleted.

    Args:
        destination_qbit: An authenticated qBittorrent client instance for the
                          destination.
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
    """Recovers incomplete cached transfers from previous runs.

    Scans for cache directories and checks if the corresponding torrent exists on
    the destination. If a cache exists but the torrent is not on the destination,
    it is considered an incomplete transfer. This function fetches the torrent
    details from the source client and adds it back to the processing queue.

    Args:
        source_qbit: An authenticated qBittorrent client for the source.
        destination_qbit: An authenticated qBittorrent client for the destination.

    Returns:
        A list of `qbittorrentapi.TorrentDictionary` objects for the torrents
        that have been recovered and should be re-processed.
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
    """Performs pre-transfer checks on the destination path.

    This function verifies two critical conditions before transfers begin:
    1.  Sufficient disk space is available for all torrents in the current run.
    2.  The application has write permissions at the destination path.

    It handles both local and remote (SFTP) destinations.

    Args:
        config: The application's configuration object.
        total_transfer_size_bytes: The total size in bytes of all files to be
                                   transferred in this run.
        ssh_connection_pools: A dictionary of SSH connection pools, used for
                              remote checks.

    Returns:
        True if all health checks pass, False otherwise.
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
    """Changes the ownership of a file or directory, locally or remotely.

    Constructs and executes a `chown` command. If `remote_config` is provided,
    the command is run via SSH; otherwise, it is run locally as a subprocess.

    Args:
        path_to_change: The path to the file or directory.
        user: The username to set as the owner. Can be empty.
        group: The group to set as the owner. Can be empty.
        remote_config: Configuration section for the remote server. If provided,
                       the operation is performed remotely.
        dry_run: If True, logs the action without executing it.
        ssh_connection_pools: A dictionary of SSH pools for remote operations.
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

def setup_logging(script_dir: Path, dry_run: bool, test_run: bool, debug: bool) -> None:
    """Configures logging to a file.

    This function sets up a file-based logger. Console logging handlers
    (like `RichHandler` or `StreamHandler`) are expected to be added later
    in the main application entry point, based on the selected UI mode.

    Args:
        script_dir: The base directory of the script, used to create a 'logs' subdir.
        dry_run: If True, a warning is logged.
        test_run: If True, a warning is logged.
        debug: If True, the log level is set to DEBUG; otherwise, INFO.
    """
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

    # --- Setup File Handler ---
    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # --- Remove all console handler logic ---
    # Console handlers will be added in main()

    logging.getLogger("paramiko").setLevel(logging.WARNING)
    logging.info("--- Torrent Mover script started (logging to file) ---")
    # --- END OF MODIFICATIONS ---

    if dry_run:
        logging.warning("!!! DRY RUN MODE ENABLED. NO CHANGES WILL BE MADE. !!!")
    if test_run:
        logging.warning("!!! TEST RUN MODE ENABLED. SOURCE TORRENTS WILL NOT BE DELETED. !!!")

def pid_exists(pid: int) -> bool:
    """Checks if a process with the given PID is currently running.

    Args:
        pid: The process ID to check.

    Returns:
        True if the process exists, False otherwise.
    """
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
    """Tests write permissions for a path by creating and deleting a temp file.

    This function can test both local and remote paths. For remote paths, it
    uses SFTP to perform the file operations.

    Args:
        path_to_test: The local or remote directory path to test.
        remote_config: The config section for the remote server. If provided,
                       the test is performed remotely.
        ssh_connection_pools: A dictionary of SSH pools for remote tests.

    Returns:
        True if write permissions are confirmed, False otherwise.
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

def delete_destination_content(
    dest_content_path: str,
    transfer_mode: str,
    ssh_connection_pools: Dict[str, SSHConnectionPool]
) -> None:
    """Deletes content from the destination, either locally or remotely.

    This is typically used to clean up failed transfers to allow for a fresh
    re-transfer on a subsequent run. It intelligently uses `rm -rf` for remote
    directories, `sftp.remove` for remote files, and `shutil`/`pathlib` for
    local deletions.

    Args:
        dest_content_path: The path to the file or directory to delete.
        transfer_mode: The current transfer mode, used to determine if the path
                       is local or remote.
        ssh_connection_pools: A dictionary of SSH pools for remote operations.

    Raises:
        Exception: Re-raises exceptions that occur during the deletion process.
    """
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
                        stdin, stdout, stderr = ssh.exec_command(command, timeout=Timeouts.SSH_EXEC) # <-- Corrected reference
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
                logging.debug("Destination is a file. Using 'p.unlink()'.")
                p.unlink()
            else:
                logging.warning(f"Destination content not found (already deleted?): {dest_content_path}")

        logging.info(f"Successfully deleted destination content: {dest_content_path}")

    except Exception as e:
        logging.error(f"An error occurred while deleting destination content: {e}", exc_info=True)
        raise # Re-raise the exception to be caught by the calling function
