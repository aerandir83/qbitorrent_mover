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
import ssh_manager
import sys

from ssh_manager import SSHConnectionPool
from transfer_manager import Timeouts
from rich.logging import RichHandler
from rich.console import Console
from clients.base import TorrentClient

if TYPE_CHECKING:
    import qbittorrentapi

class LockFile:
    """Ensures that only one instance of the script can run at a time.

    This class uses the `fcntl` module to create an atomic, advisory lock on a
    file. This prevents race conditions and ensures that operations are not
    disrupted by another instance of the same script starting. The lock is
    automatically released upon script exit. This implementation is suitable
    for Unix-like systems.

    Attributes:
        lock_path: The Path object for the lock file.
        lock_fd: The file descriptor for the opened lock file.
    """

    def __init__(self, lock_path: Path):
        """Initializes the LockFile.

        Args:
            lock_path: The path to the file that will be used for locking.
        """
        self.lock_path = lock_path
        self.lock_fd: Optional[int] = None
        self._acquired = False

    def acquire(self) -> None:
        """Acquires an exclusive, non-blocking lock, handling stale locks.

        If the lock file exists, it reads the PID and checks if the process
        is still running. If not, the stale lock is removed. If the process
        is running, it raises a RuntimeError.

        Raises:
            RuntimeError: If the lock file is already locked by another live process.
        """
        if self.lock_path.exists():
            pid_str = self.get_locking_pid()
            if pid_str and pid_str.isdigit():
                pid = int(pid_str)
                if pid_exists(pid):
                    raise RuntimeError(f"Script is already running with PID {pid} (lock file: {self.lock_path})")
                else:
                    logging.warning(f"Removing stale lock file for PID {pid} that is no longer running.")
                    self.lock_path.unlink()
            else:
                # Handle empty or non-numeric pid_str
                if pid_str is not None:
                    logging.warning(f"Removing corrupt lock file with invalid PID: '{pid_str}'.")
                self.lock_path.unlink(missing_ok=True)

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
            # This part should theoretically not be reached if the stale lock check works,
            # but as a fallback it's good to keep.
            pid = self.get_locking_pid()
            if pid:
                raise RuntimeError(f"Script is already running with PID {pid} (lock file: {self.lock_path})")
            else:
                raise RuntimeError(f"Script is already running (lock file: {self.lock_path})")

    def release(self) -> None:
        """Releases the lock and cleans up the lock file.

        This method removes the lock, closes the file descriptor, and deletes
        the lock file from the filesystem. It is registered with `atexit` to
        ensure it is called automatically on script termination.
        """
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
        """Reads the PID of the process holding the lock.

        Returns:
            The process ID (PID) as a string if the lock file exists and is
            readable, otherwise None.
        """
        if self.lock_path.exists():
            try:
                return self.lock_path.read_text().strip()
            except IOError:
                return None
        return None

# --- System & Health Check Functions ---

def cleanup_orphaned_cache(destination_client: TorrentClient) -> None:
    """Removes orphaned local cache directories from previous runs.

    This function scans the system's temporary directory for cache folders
    created by this script. If a cache folder corresponds to a torrent that
    is already completed on the destination qBittorrent client, the cache
    folder is deleted to free up disk space.

    Args:
        destination_client: An authenticated TorrentClient instance for the
            destination client.
    """
    temp_dir = Path(tempfile.gettempdir())
    cache_prefix = "torrent_mover_cache_"
    logging.info("--- Running Orphaned Cache Cleanup ---")
    try:
        # ToDo: Expand Interface - TorrentClient needs generic get_torrents(filter)
        if not (hasattr(destination_client, 'client') and destination_client.client):
             logging.warning("Destination client does not expose underlying 'client' object. Skipping cache cleanup.")
             return

        all_dest_hashes: Set[str] = {t.hash for t in destination_client.client.torrents_info() if t.progress == 1}
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

def recover_cached_torrents(source_client: TorrentClient, destination_client: TorrentClient) -> List["qbittorrentapi.TorrentDictionary"]:
    """Identifies and recovers torrents from incomplete cached transfers.

    This function scans the temporary directory for cache folders from previous
    runs. If a cache folder's torrent does not exist on the destination client,
    it is considered a failed or incomplete transfer. This function retrieves
    the full torrent information from the source client and returns it so it
    can be added back into the processing queue for a retry.

    Args:
        source_client: An authenticated TorrentClient instance for the source.
        destination_client: An authenticated TorrentClient instance for the
            destination.

    Returns:
        A list of TorrentDictionary objects for the torrents that need to be
        recovered and re-processed.
    """
    temp_dir = Path(tempfile.gettempdir())
    cache_prefix = "torrent_mover_cache_"
    logging.info("--- Checking for incomplete cached transfers to recover ---")

    # ToDo: Expand Interface - TorrentClient needs generic get_torrents() and access to underlying client
    if not (hasattr(source_client, 'client') and source_client.client and hasattr(destination_client, 'client') and destination_client.client):
         logging.warning("Clients do not expose underlying 'client' object. Skipping cache recovery.")
         return []

    try:
        all_dest_hashes: Set[str] = {t.hash for t in destination_client.client.torrents_info()}
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
        source_torrents = source_client.client.torrents_info(torrent_hashes=hashes_to_recover)
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
    """Verifies that the destination is ready for file transfers.

    This function performs two key checks:
    1.  Disk Space: Ensures there is enough free space at the destination path
        for the current batch of torrents. It handles both local and remote
        (via SSH `df` command) checks.
    2.  Permissions: Calls `test_path_permissions` to verify that the script
        has write access to the destination path.

    Args:
        config: The application's ConfigParser object.
        total_transfer_size_bytes: The total size in bytes of all files to be
            transferred in the current run.
        ssh_connection_pools: A dictionary of SSHConnectionPool objects, used
            for remote checks.

    Returns:
        True if all health checks pass, False otherwise.
    """
    logging.info("--- Running Destination Health Check ---")
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    dest_path = config['DESTINATION_PATHS'].get('destination_path')
    remote_config = config['DESTINATION_SERVER'] if transfer_mode in ['sftp_upload', 'rsync_upload'] and 'DESTINATION_SERVER' in config else None
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

def change_ownership(path_to_change: str, user: str, group: str, remote_config: Optional[configparser.SectionProxy] = None, dry_run: bool = False, ssh_connection_pools: Optional[Dict[str, SSHConnectionPool]] = None) -> bool:
    """Changes the ownership of a file or directory.

    This function can operate on both local and remote filesystems. If
    `remote_config` and `ssh_connection_pools` are provided, it executes `chown`
    over SSH. Otherwise, it executes `chown` locally using `subprocess`.

    Args:
        path_to_change: The path to the file or directory.
        user: The username to set as the owner.
        group: The group name to set as the owner.
        remote_config: The configuration section for the remote server. If None,
            the operation is assumed to be local.
        dry_run: If True, logs the command that would be executed without
            actually running it.
        ssh_connection_pools: A dictionary of SSH connection pools, required for
            remote operations.

    Returns:
        True if the operation was successful or not required (dry run, no user/group set).
        False if the chown command failed (non-zero exit code or exception).
    """
    if not user and not group:
        logging.debug("No chown user/group configured. Skipping ownership change.")
        return True
    owner_spec = f"{user or ''}:{group or ''}".strip(':')
    if dry_run:
        logging.info(f"[DRY RUN] Would change ownership of '{path_to_change}' to '{owner_spec}'.")
        return True
    if remote_config and ssh_connection_pools:
        logging.info(f"Attempting to change remote ownership of '{path_to_change}' to '{owner_spec}'...")
        # Explicitly use the DESTINATION_SERVER pool, as this is the only remote chown context
        pool = ssh_connection_pools.get('DESTINATION_SERVER')
        if not pool:
            logging.error("Could not find SSH pool for DESTINATION_SERVER for chown operation.")
            return False

        try:
            with pool.get_connection() as (sftp, ssh):
                # Using shlex.quote to prevent command injection vulnerabilities
                remote_command = f"chown -R -- {shlex.quote(owner_spec)} {shlex.quote(path_to_change)}"
                logging.debug(f"Executing remote command: {remote_command}")
                stdin, stdout, stderr = ssh.exec_command(remote_command, timeout=Timeouts.SSH_EXEC)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status == 0:
                    logging.info(f"Remote ownership changed successfully for '{path_to_change}'.")
                    return True
                else:
                    stderr_output = stderr.read().decode('utf-8').strip()
                    logging.error(f"Failed to change remote ownership for '{path_to_change}'. Exit code: {exit_status}, Stderr: {stderr_output}")
                    return False
        except Exception as e:
            logging.error(f"An exception occurred during remote chown for '{path_to_change}': {e}", exc_info=True)
            return False
    else:
        logging.info(f"Attempting to change local ownership of '{path_to_change}' to '{owner_spec}'...")
        try:
            if shutil.which("chown") is None:
                logging.warning("'chown' command not found locally. Skipping ownership change.")
                return False
            command = ["chown", "-R", owner_spec, path_to_change]
            process = subprocess.run(command, capture_output=True, text=True, check=False)
            if process.returncode == 0:
                logging.info("Local ownership changed successfully.")
                return True
            else:
                logging.error(f"Failed to change local ownership for '{path_to_change}'. Exit code: {process.returncode}, Stderr: {process.stderr.strip()}")
                return False
        except Exception as e:
            logging.error(f"An exception occurred during local chown: {e}", exc_info=True)
            return False

def setup_logging(script_dir: Path, dry_run: bool, test_run: bool, debug: bool) -> None:
    """Configures the root logger for file-based logging.

    This function sets up a rotating file handler that logs messages to a
    timestamped file in the 'logs' directory. It sets the base logging level
    for the application. Console logging (e.g., RichHandler or StreamHandler)
    is configured separately in the `main` function.

    Args:
        script_dir: The directory where the main script is located.
        dry_run: If True, adds a warning to the log.
        test_run: If True, adds a warning to the log.
        debug: If True, sets the logging level to DEBUG, otherwise INFO.
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

    This function uses `os.kill` with a signal of 0, which does not actually
    send a signal but does perform error checking. This is a reliable way to
    check for the existence of a process on Unix-like systems.

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
    """Tests for write permissions at a specified local or remote path.

    This function attempts to create and then delete a temporary file at the
    given path to verify that the user running the script has the necessary
    permissions. It can operate in two modes:
    1.  Local: If `remote_config` is not provided.
    2.  Remote: If `remote_config` and `ssh_connection_pools` are provided,
        the test is performed over SFTP.

    Args:
        path_to_test: The absolute path to the directory where permissions should
            be checked.
        remote_config: The configuration section for the remote server. If None,
            the test is performed locally.
        ssh_connection_pools: A dictionary of SSH connection pools, required for
            remote tests.

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
    """Deletes content from the destination path.

    This function handles the deletion of files or directories at the destination,
    adapting the deletion method based on the transfer mode.
    -   For remote destinations (`sftp_upload`), it uses SSH `rm -rf` for
        directories and SFTP `remove` for files.
    -   For local destinations (`sftp`, `rsync`), it uses `shutil.rmtree` for
        directories and `Path.unlink` for files.

    Args:
        dest_content_path: The absolute path to the content to be deleted.
        transfer_mode: The current transfer mode (e.g., 'sftp_upload').
        ssh_connection_pools: A dictionary of SSH connection pools, required for
            remote deletion.

    Raises:
        Exception: Propagates exceptions that occur during the deletion process.
    """
    is_remote = (transfer_mode in ['sftp_upload', 'rsync_upload'])

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
            p_str = str(dest_content_path)
            p = Path(p_str)
            if p.exists():
                # New strategy: Move to a temp "trash" dir, then delete.
                # This is much more robust on network filesystems (NFS, SMB)
                # which can fail `rm -rf` with "Directory not empty" on stale handles.
                try:
                    trash_name = f"torrent_mover_trash_{p.name}_{int(time.time())}"
                    trash_path = Path(tempfile.gettempdir()) / trash_name

                    logging.debug(f"Moving '{p_str}' to trash: '{trash_path}'")
                    shutil.move(p_str, trash_path)

                    # Now that it's "unlinked" from the network mount, delete it.
                    logging.debug(f"Deleting from trash: '{trash_path}'")
                    # We can use shutil.rmtree here as it's now in a local temp dir
                    shutil.rmtree(trash_path, ignore_errors=True)

                    # As a fallback, run rm -rf if shutil fails
                    if trash_path.exists():
                        logging.warning(f"shutil.rmtree failed on trash dir, falling back to 'rm -rf {trash_path}'")
                        subprocess.run(["rm", "-rf", str(trash_path)], capture_output=True, text=True)

                except Exception as e:
                    logging.error(f"Failed to delete local content '{p_str}' via move-to-trash. Error: {e}")
                    logging.error("Falling back to direct 'rm -rf'...")
                    try:
                        subprocess.run(["rm", "-rf", p_str], check=True, capture_output=True, text=True)
                    except subprocess.CalledProcessError as sub_e:
                        logging.error(f"Fallback 'rm -rf' also failed for '{p_str}'. Stderr: {sub_e.stderr.strip()}")
                        raise sub_e # Re-raise the rm -rf error
                    except Exception as final_e:
                        logging.error(f"Fallback 'rm -rf' failed with unexpected error: {final_e}")
                        raise final_e
            else:
                logging.warning(f"Destination content not found (already deleted?): {dest_content_path}")

        logging.info(f"Successfully deleted destination content: {dest_content_path}")

    except Exception as e:
        logging.error(f"An error occurred while deleting destination content: {e}", exc_info=True)
        raise # Re-raise the exception to be caught by the calling function


def delete_destination_files(
    base_path: str,
    relative_file_paths: List[str],
    transfer_mode: str,
    ssh_connection_pools: Dict[str, SSHConnectionPool]
) -> None:
    """
    Deletes a specific list of files from the destination path.

    This function handles both local and remote deletion based on the transfer_mode.

    Args:
        base_path: The absolute root path of the torrent content.
        relative_file_paths: A list of file paths, relative to the base_path,
            that should be deleted.
        transfer_mode: The current transfer mode (e.g., 'sftp_upload', 'rsync').
        ssh_connection_pools: A dictionary of SSH connection pools, required for
            remote deletion.
    """
    is_remote = (transfer_mode in ['sftp_upload', 'rsync_upload'])
    logging.warning(f"Attempting to delete {len(relative_file_paths)} specific files from: {base_path}")

    if is_remote:
        # Remote deletion
        dest_pool = ssh_connection_pools.get('DESTINATION_SERVER')
        if not dest_pool:
            logging.error("Cannot delete remote files: Destination SSH pool not found.")
            return

        try:
            with dest_pool.get_connection() as (sftp, ssh):
                for rel_path in relative_file_paths:
                    # Ensure we use forward slashes for remote paths
                    full_path = (Path(base_path) / rel_path).as_posix()
                    logging.debug(f"Deleting remote file: {full_path}")
                    try:
                        sftp.remove(full_path)
                    except FileNotFoundError:
                        logging.warning(f"File not found (already deleted?): {full_path}")
                    except Exception as e:
                        logging.error(f"Failed to delete remote file: {full_path}. Error: {e}")
            logging.info(f"Partial deletion of {len(relative_file_paths)} remote files complete.")
        except Exception as e:
            logging.error(f"An error occurred during remote partial file deletion: {e}", exc_info=True)

    else:
        # Local deletion
        for rel_path in relative_file_paths:
            full_path = Path(base_path) / rel_path
            logging.debug(f"Deleting local file: {full_path}")
            try:
                full_path.unlink(missing_ok=True)
            except Exception as e:
                # This could be a PermissionError or IsADirectoryError, etc.
                logging.error(f"Failed to delete local file: {full_path}. Error: {e}")
        logging.info(f"Partial deletion of {len(relative_file_paths)} local files complete.")
