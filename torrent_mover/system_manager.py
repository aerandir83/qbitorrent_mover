"""Manages system-level operations and pre-transfer checks.

This module provides functions for interacting with the underlying operating system,
both locally and remotely. Its responsibilities include:
- Ensuring script singleton status using a file-based lock (`LockFile`).
- Setting up and configuring file-based logging.
- Performing pre-transfer "health checks" on the destination, such as verifying
  disk space and write permissions.
- Cleaning up temporary cache files from previous failed or incomplete runs.
- Recovering and re-queueing torrents from orphaned cache directories.
- Handling file and directory ownership changes (`chown`).
- Deleting content from local or remote destinations.
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
from typing import Optional, Dict, List, Set, TYPE_CHECKING
import configparser
from .ssh_manager import SSHConnectionPool
from .transfer_manager import Timeouts

if TYPE_CHECKING:
    import qbittorrentapi

class LockFile:
    """Ensures that only one instance of the script can run at a time on Unix-like systems.

    This class uses the `fcntl` module to create an advisory lock on a specified
    file. This prevents race conditions that could occur if multiple instances of
    the script attempt to process the same torrents simultaneously.

    The lock is automatically released when the script exits gracefully. The class
    also includes logic to detect and remove stale lock files left behind by
    crashed processes.

    Attributes:
        lock_path (Path): The path to the file used for locking.
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
        """Acquires an exclusive, non-blocking lock on the lock file.

        This method first checks if a lock file already exists. If it does, it
        reads the PID from the file and checks if that process is still running.
        If the process is not running, the stale lock file is removed. If the
        process is still active, a `RuntimeError` is raised.

        If no active lock is found, it creates a new lock file, writes the current
        process's PID to it, and acquires an exclusive lock.

        Raises:
            RuntimeError: If the lock is already held by another running instance.
        """
        if self.lock_path.exists():
            pid_str = self.get_locking_pid()
            if pid_str and pid_str.isdigit():
                pid = int(pid_str)
                if pid_exists(pid):
                    raise RuntimeError(f"Script is already running with PID {pid} (lock file: {self.lock_path})")
                else:
                    logging.warning(f"Removing stale lock file for non-existent PID {pid}.")
                    self.lock_path.unlink()
            else:
                logging.warning(f"Removing corrupt lock file with invalid PID: '{pid_str}'.")
                self.lock_path.unlink(missing_ok=True)

        try:
            self.lock_fd = open(self.lock_path, 'w')
            fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.lock_fd.write(str(os.getpid()))
            self.lock_fd.flush()
            atexit.register(self.release)
            self._acquired = True
        except (IOError, BlockingIOError):
            if self.lock_fd:
                self.lock_fd.close()
            pid = self.get_locking_pid()
            raise RuntimeError(f"Script is already running with PID {pid} (lock file: {self.lock_path})")

    def release(self) -> None:
        """Releases the file lock and deletes the lock file.

        This method is registered with `atexit` to be called automatically on
        script termination, ensuring the lock is always released.
        """
        if self.lock_fd and self._acquired:
            try:
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
                self.lock_fd.close()
                self.lock_path.unlink(missing_ok=True)
                self._acquired = False
            except Exception as e:
                logging.error(f"Failed to release lock file '{self.lock_path}': {e}")
            finally:
                self.lock_fd = None

    def get_locking_pid(self) -> Optional[str]:
        """Reads the PID of the process that currently holds the lock.

        Returns:
            The process ID (PID) as a string if the lock file exists and is
            readable, otherwise `None`.
        """
        if self.lock_path.exists():
            try:
                return self.lock_path.read_text().strip()
            except IOError:
                return None
        return None

def cleanup_orphaned_cache(destination_qbit: "qbittorrentapi.Client") -> None:
    """Removes leftover local cache directories from previous runs.

    This function scans the system's temporary directory for cache folders created
    by this script (`torrent_mover_cache_*`). If a cache folder's corresponding
    torrent hash is found to be already completed on the destination client, the
    cache folder is deemed unnecessary and is deleted to free up disk space.

    Args:
        destination_qbit: An authenticated qBittorrent client instance for the
            destination, used to check for completed torrents.
    """
    temp_dir = Path(tempfile.gettempdir())
    cache_prefix = "torrent_mover_cache_"
    logging.info("--- Running Orphaned Cache Cleanup ---")
    try:
        all_dest_hashes: Set[str] = {t.hash for t in destination_qbit.torrents_info() if t.progress == 1}
        logging.info(f"Found {len(all_dest_hashes)} completed torrent hashes on destination for cleanup check.")
    except Exception as e:
        logging.error(f"Could not retrieve torrents from destination for cache cleanup: {e}")
        return

    found_cache_dirs = 0
    cleaned_cache_dirs = 0
    for item in temp_dir.iterdir():
        if item.is_dir() and item.name.startswith(cache_prefix):
            found_cache_dirs += 1
            torrent_hash = item.name[len(cache_prefix):]
            if torrent_hash in all_dest_hashes:
                logging.warning(f"Found orphaned cache for a completed torrent ({torrent_hash[:10]}...). Removing.")
                try:
                    shutil.rmtree(item)
                    cleaned_cache_dirs += 1
                except OSError as e:
                    logging.error(f"Failed to remove orphaned cache directory '{item}': {e}")
            else:
                logging.debug(f"Found valid cache for an incomplete torrent ({torrent_hash[:10]}...). Preserving.")

    if found_cache_dirs == 0:
        logging.info("No leftover cache directories found.")
    else:
        logging.info(f"Cleanup complete. Removed {cleaned_cache_dirs}/{found_cache_dirs} orphaned cache directories.")

def recover_cached_torrents(source_qbit: "qbittorrentapi.Client", destination_qbit: "qbittorrentapi.Client") -> List["qbittorrentapi.TorrentDictionary"]:
    """Identifies and recovers torrents from incomplete cached transfers.

    This function scans the temporary directory for cache folders. If a cache
    folder exists but its corresponding torrent is not on the destination client,
    it's considered an incomplete transfer. The function then retrieves the full
    torrent information from the source client so it can be re-added to the
    processing queue.

    Args:
        source_qbit: An authenticated qBittorrent client instance for the source.
        destination_qbit: An authenticated qBittorrent client instance for the destination.

    Returns:
        A list of `TorrentDictionary` objects for torrents that need to be
        recovered and re-processed.
    """
    temp_dir = Path(tempfile.gettempdir())
    cache_prefix = "torrent_mover_cache_"
    logging.info("--- Checking for incomplete cached transfers to recover ---")
    try:
        all_dest_hashes: Set[str] = {t.hash for t in destination_qbit.torrents_info()}
    except Exception as e:
        logging.error(f"Could not retrieve torrents from destination for cache recovery: {e}")
        return []

    cache_dirs = [item for item in temp_dir.iterdir() if item.is_dir() and item.name.startswith(cache_prefix)]
    if not cache_dirs:
        logging.info("No leftover cache directories found to recover.")
        return []

    hashes_to_recover = [item.name[len(cache_prefix):] for item in cache_dirs if item.name[len(cache_prefix):] not in all_dest_hashes]
    if not hashes_to_recover:
        logging.info("All found cache directories correspond to existing torrents on destination.")
        return []

    logging.warning(f"Found {len(hashes_to_recover)} incomplete cached transfers. Attempting to recover.")
    try:
        source_torrents = source_qbit.torrents_info(torrent_hashes=hashes_to_recover)
        recovered_torrents = list(source_torrents)
        if recovered_torrents:
            logging.info(f"Successfully recovered {len(recovered_torrents)} torrent(s) from cache. They will be added to the queue.")
        else:
            logging.warning("Could not find matching torrents on the source for the incomplete caches.")
        return recovered_torrents
    except Exception as e:
        logging.error(f"An error occurred while getting torrent info from source for recovery: {e}")
        return []

def destination_health_check(config: configparser.ConfigParser, total_transfer_size_bytes: int, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> bool:
    """Verifies that the destination is ready for file transfers.

    This function performs two critical checks before starting transfers:
    1.  Disk Space: Ensures there is enough free space at the destination path for
        the current batch of torrents. It supports both local and remote (via SSH)
        disk space checks.
    2.  Permissions: Verifies that the script has write access to the destination path.

    Args:
        config: The application's `ConfigParser` object.
        total_transfer_size_bytes: The total size in bytes of all files to be
            transferred in the current run.
        ssh_connection_pools: A dictionary of `SSHConnectionPool` objects, used for
            remote health checks.

    Returns:
        `True` if all health checks pass, `False` otherwise.
    """
    logging.info("--- Running Destination Health Check ---")
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    dest_path = config['DESTINATION_PATHS']['destination_path']
    is_remote = transfer_mode in ['sftp_upload', 'rsync_upload'] and 'DESTINATION_SERVER' in config
    remote_config = config['DESTINATION_SERVER'] if is_remote else None

    # --- 1. Disk Space Check ---
    try:
        available_space = _get_available_space(dest_path, is_remote, ssh_connection_pools)
        required_space_gb = total_transfer_size_bytes / (1024**3)
        available_space_gb = available_space / (1024**3)
        logging.info(f"Required space: {required_space_gb:.2f} GB. Available on destination: {available_space_gb:.2f} GB.")
        if total_transfer_size_bytes > available_space:
            logging.error("FATAL: Not enough disk space on the destination.")
            return False
        logging.info("[green]SUCCESS:[/] Destination has enough disk space.")
    except (FileNotFoundError, ValueError) as e:
        logging.error(f"FATAL: Disk space check failed. The destination path '{dest_path}' may not exist or is inaccessible. Error: {e}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred during disk space check: {e}", exc_info=True)
        return False

    # --- 2. Permissions Check ---
    if not test_path_permissions(dest_path, remote_config=remote_config, ssh_connection_pools=ssh_connection_pools):
        logging.error("FATAL: Destination permission check failed.")
        return False

    logging.info("--- Destination Health Check Passed ---")
    return True

def _get_available_space(path: str, is_remote: bool, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> int:
    """Helper function to get available disk space for local or remote paths."""
    if is_remote:
        dest_pool = ssh_connection_pools['DESTINATION_SERVER']
        with dest_pool.get_connection() as (_sftp, ssh):
            command = f"df -kP {shlex.quote(path)}"
            _stdin, stdout, stderr = ssh.exec_command(command, timeout=Timeouts.SSH_EXEC)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise ValueError(f"'df' command failed: {stderr.read().decode().strip()}")
            output = stdout.read().decode('utf-8').strip().splitlines()
            if len(output) < 2:
                raise ValueError(f"Could not parse 'df' output: '{' '.join(output)}'")
            parts = output[1].split()
            return int(parts[3]) * 1024
    else:
        if not Path(path).exists():
            raise FileNotFoundError(f"Local path '{path}' does not exist.")
        stats = os.statvfs(path)
        return stats.f_bavail * stats.f_frsize

def change_ownership(path_to_change: str, user: str, group: str, remote_config: Optional[configparser.SectionProxy] = None, dry_run: bool = False, ssh_connection_pools: Optional[Dict[str, SSHConnectionPool]] = None) -> None:
    """Changes the ownership of a file or directory, locally or remotely.

    If `remote_config` and `ssh_connection_pools` are provided, it executes `chown`
    over SSH. Otherwise, it executes `chown` locally using `subprocess`.

    Args:
        path_to_change: The path of the file or directory.
        user: The username to set as the new owner. Can be empty.
        group: The group name to set as the new owner. Can be empty.
        remote_config: The configuration section for the remote server. If `None`,
            the operation is performed locally.
        dry_run: If `True`, logs the command without executing it.
        ssh_connection_pools: A dictionary of SSH connection pools, required for
            remote operations.
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
            logging.error("Could not find SSH pool for DESTINATION_SERVER to change ownership.")
            return
        try:
            with pool.get_connection() as (_sftp, ssh):
                remote_command = f"chown -R -- {shlex.quote(owner_spec)} {shlex.quote(path_to_change)}"
                _stdin, stdout, stderr = ssh.exec_command(remote_command, timeout=Timeouts.SSH_EXEC)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status == 0:
                    logging.info("Remote ownership changed successfully.")
                else:
                    stderr_output = stderr.read().decode('utf-8').strip()
                    logging.error(f"Failed to change remote ownership. Exit code: {exit_status}, Stderr: {stderr_output}")
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
                logging.error(f"Failed to change local ownership. Exit code: {process.returncode}, Stderr: {process.stderr.strip()}")
        except Exception as e:
            logging.error(f"An exception occurred during local chown: {e}", exc_info=True)

def setup_logging(script_dir: Path, dry_run: bool, test_run: bool, debug: bool) -> None:
    """Configures the root logger for file-based logging.

    This function sets up a `FileHandler` that logs messages to a timestamped
    file in a 'logs' subdirectory. It sets the base logging level for the
    application. Console-specific logging handlers (like RichHandler) are
    configured separately in the main entry point.

    Args:
        script_dir: The directory where the main script is located.
        dry_run: If `True`, a prominent warning is added to the log.
        test_run: If `True`, a prominent warning is added to the log.
        debug: If `True`, sets the logging level to `DEBUG`, otherwise `INFO`.
    """
    log_dir = script_dir / 'logs'
    log_dir.mkdir(exist_ok=True)
    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = log_dir / f"torrent_mover_{timestamp}.log"

    logger = logging.getLogger()
    log_level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(log_level)

    if logger.hasHandlers():
        logger.handlers.clear()

    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    logging.getLogger("paramiko").setLevel(logging.WARNING)
    logging.info("--- Torrent Mover file logging started ---")

    if dry_run:
        logging.warning("!!! DRY RUN MODE ENABLED. NO CHANGES WILL BE MADE. !!!")
    if test_run:
        logging.warning("!!! TEST RUN MODE ENABLED. SOURCE TORRENTS WILL NOT BE DELETED. !!!")

def pid_exists(pid: int) -> bool:
    """Checks if a process with the given PID is currently running on a Unix-like system.

    This function uses `os.kill` with a signal of 0, which doesn't actually send
    a signal but performs error checking to determine if the process exists.

    Args:
        pid: The process ID to check.

    Returns:
        `True` if a process with the given PID exists, `False` otherwise.
    """
    if pid <= 0: return False
    try:
        os.kill(pid, 0)
    except OSError as err:
        return err.errno == errno.EPERM
    else:
        return True

def test_path_permissions(path_to_test: str, remote_config: Optional[configparser.SectionProxy] = None, ssh_connection_pools: Optional[Dict[str, SSHConnectionPool]] = None) -> bool:
    """Tests for write permissions at a specified local or remote directory.

    This function attempts to create and then delete a temporary file in the
    target directory to confirm that the user running the script has the necessary
    permissions. It supports both local filesystem paths and remote SFTP paths.

    Args:
        path_to_test: The absolute path to the directory for the permission test.
        remote_config: The configuration section for the remote server. If `None`,
            the test is performed on the local filesystem.
        ssh_connection_pools: A dictionary of SSH connection pools, required for
            remote permission tests.

    Returns:
        `True` if write permissions are confirmed, `False` otherwise.
    """
    is_remote = remote_config is not None
    logging.info(f"--- Running {'REMOTE' if is_remote else 'LOCAL'} Permission Test on: {path_to_test} ---")

    if is_remote:
        if not ssh_connection_pools:
            logging.error("SSH connection pools not provided for remote permission test.")
            return False
        pool = ssh_connection_pools.get('DESTINATION_SERVER')
        if not pool:
            logging.error("Could not find SSH pool for DESTINATION_SERVER.")
            return False
        try:
            with pool.get_connection() as (sftp, _ssh):
                test_file_path = f"{path_to_test.replace('\\', '/').rstrip('/')}/permission_test_{os.getpid()}.tmp"
                logging.info(f"Attempting to create remote test file: {test_file_path}")
                with sftp.open(test_file_path, 'w') as f:
                    f.write('test')
                sftp.remove(test_file_path)
            logging.info(f"[bold green]SUCCESS:[/] Remote write permissions are correctly configured.")
            return True
        except PermissionError:
            logging.error(f"[bold red]FAILURE:[/] Permission denied on remote server for path '{path_to_test}'.")
            return False
        except Exception as e:
            logging.error(f"[bold red]FAILURE:[/] An unexpected error occurred during remote permission test: {e}", exc_info=True)
            return False
    else:
        p = Path(path_to_test)
        if not p.exists() or not p.is_dir():
            logging.error(f"Error: The local path '{p}' does not exist or is not a directory.")
            return False
        test_file_path = p / f"permission_test_{os.getpid()}.tmp"
        try:
            with open(test_file_path, 'w') as f:
                f.write('test')
            os.remove(test_file_path)
            logging.info(f"[bold green]SUCCESS:[/] Local write permissions are correctly configured.")
            return True
        except PermissionError:
            logging.error(f"[bold red]FAILURE:[/] Permission denied for local path '{p}'.")
            return False
        except Exception as e:
            logging.error(f"[bold red]FAILURE:[/] An unexpected error occurred during local permission test: {e}", exc_info=True)
            return False

def delete_destination_content(dest_content_path: str, transfer_mode: str, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> None:
    """Deletes a file or directory at the destination, either locally or remotely.

    This function adapts its deletion method based on the `transfer_mode`. For
    remote destinations, it uses SSH `rm -rf`. For local destinations, it uses a
    more robust strategy of moving the content to a temporary "trash" directory
    before deleting it, which is more reliable on network filesystems like NFS or SMB.

    Args:
        dest_content_path: The absolute path to the content to be deleted.
        transfer_mode: The current transfer mode (e.g., 'sftp_upload', 'sftp').
        ssh_connection_pools: A dictionary of SSH connection pools, required for
            remote deletion.

    Raises:
        Exception: Propagates exceptions that occur during the deletion process.
    """
    is_remote = transfer_mode in ['sftp_upload', 'rsync_upload']
    logging.warning(f"Attempting to delete destination content: {dest_content_path}")

    try:
        if is_remote:
            _delete_remote_content(dest_content_path, ssh_connection_pools)
        else:
            _delete_local_content(dest_content_path)
        logging.info(f"Successfully deleted destination content: {dest_content_path}")
    except Exception as e:
        logging.error(f"An error occurred while deleting destination content: {e}", exc_info=True)
        raise

def _delete_remote_content(path: str, pools: Dict[str, SSHConnectionPool]) -> None:
    """Helper for remote content deletion."""
    dest_pool = pools.get('DESTINATION_SERVER')
    if not dest_pool:
        raise ValueError("Cannot delete remote content: Destination SSH pool not found.")

    with dest_pool.get_connection() as (sftp, ssh):
        try:
            sftp.stat(path) # Check for existence
            command = f"rm -rf {shlex.quote(path)}"
            _stdin, stdout, stderr = ssh.exec_command(command, timeout=Timeouts.SSH_EXEC)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                raise IOError(f"Failed to delete remote content: {stderr.read().decode()}")
        except FileNotFoundError:
            logging.warning(f"Remote content not found (already deleted?): {path}")

def _delete_local_content(path_str: str) -> None:
    """Helper for robust local content deletion."""
    p = Path(path_str)
    if not p.exists():
        logging.warning(f"Local content not found (already deleted?): {path_str}")
        return

    try:
        # Move-to-trash strategy for network filesystem robustness
        trash_name = f"torrent_mover_trash_{p.name}_{int(time.time())}"
        trash_path = Path(tempfile.gettempdir()) / trash_name
        shutil.move(path_str, trash_path)
        shutil.rmtree(trash_path, ignore_errors=True)
        if trash_path.exists():
            subprocess.run(["rm", "-rf", str(trash_path)], check=False)
    except Exception as e:
        logging.error(f"Move-to-trash deletion failed for '{path_str}': {e}. Falling back to direct 'rm -rf'.")
        try:
            subprocess.run(["rm", "-rf", path_str], check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as sub_e:
            logging.error(f"Fallback 'rm -rf' also failed for '{path_str}'. Stderr: {sub_e.stderr.strip()}")
            raise sub_e

def delete_destination_files(base_path: str, relative_file_paths: List[str], transfer_mode: str, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> None:
    """Deletes a specific list of files from the destination, locally or remotely.

    This function is used for partial content deletion, for instance, when a
    recheck fails and only the corrupt files need to be removed before a
    delta transfer.

    Args:
        base_path: The absolute root path of the torrent content.
        relative_file_paths: A list of file paths, relative to `base_path`,
            that should be deleted.
        transfer_mode: The current transfer mode (e.g., 'sftp_upload', 'rsync').
        ssh_connection_pools: A dictionary of SSH connection pools, required for
            remote deletion.
    """
    is_remote = transfer_mode in ['sftp_upload', 'rsync_upload']
    logging.warning(f"Attempting to delete {len(relative_file_paths)} specific files from: {base_path}")

    if is_remote:
        dest_pool = ssh_connection_pools.get('DESTINATION_SERVER')
        if not dest_pool:
            logging.error("Cannot delete remote files: Destination SSH pool not found.")
            return
        try:
            with dest_pool.get_connection() as (sftp, _ssh):
                for rel_path in relative_file_paths:
                    full_path = (Path(base_path) / rel_path).as_posix()
                    try:
                        sftp.remove(full_path)
                    except FileNotFoundError:
                        logging.warning(f"File not found for deletion (already deleted?): {full_path}")
                    except Exception as e:
                        logging.error(f"Failed to delete remote file: {full_path}. Error: {e}")
            logging.info(f"Partial deletion of {len(relative_file_paths)} remote files complete.")
        except Exception as e:
            logging.error(f"An error occurred during remote partial file deletion: {e}", exc_info=True)
    else:
        for rel_path in relative_file_paths:
            full_path = Path(base_path) / rel_path
            try:
                full_path.unlink(missing_ok=True)
            except Exception as e:
                logging.error(f"Failed to delete local file: {full_path}. Error: {e}")
        logging.info(f"Partial deletion of {len(relative_file_paths)} local files complete.")
