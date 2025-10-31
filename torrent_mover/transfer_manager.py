"""Manages the core logic for file transfers and state tracking.

This module provides classes and functions to handle different transfer modes
(SFTP, rsync), manage resumable transfers, and track the state of completed
or failed torrents. It includes a `RateLimitedFile` wrapper for bandwidth
throttling and orchestrates concurrent file transfers using a thread pool.
"""
import logging
import os
import time
import subprocess
import re
import shlex
from pathlib import Path
import json
import socket
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed
from .utils import retry
from .ssh_manager import SSHConnectionPool, sftp_mkdir_p, SSH_CONTROL_PATH
from .ui import UIManagerV2 as UIManager
import threading
from typing import Any, Dict, List
import tempfile
import shutil
import configparser

if typing.TYPE_CHECKING:
    import qbittorrentapi

class RateLimitedFile:
    """A file-like object wrapper that throttles read and write speeds.

    This class wraps a file object and monitors the bytes transferred, inserting
    `time.sleep()` calls as needed to ensure the data rate does not exceed a
    specified maximum.

    Attributes:
        file: The underlying file object to wrap.
        max_bytes_per_sec: The maximum transfer speed in bytes per second.
    """
    def __init__(self, file_obj: Any, max_bytes_per_sec: float):
        """Initializes the RateLimitedFile.

        Args:
            file_obj: The file object (e.g., from `open()` or `sftp.open()`).
            max_bytes_per_sec: The maximum desired transfer rate in B/s.
                               If 0 or None, throttling is disabled.
        """
        self.file = file_obj
        self.max_bytes_per_sec = max_bytes_per_sec if max_bytes_per_sec else float('inf')
        self.last_time = time.time()
        self.bytes_since_last = 0

    def read(self, size: int) -> bytes:
        """Reads data from the file and applies throttling.

        Args:
            size: The number of bytes to read.

        Returns:
            The bytes read from the file.
        """
        data = self.file.read(size)
        self._throttle(len(data))
        return data

    def write(self, data: bytes) -> int:
        """Writes data to the file and applies throttling.

        Args:
            data: The bytes to write to the file.

        Returns:
            The number of bytes written.
        """
        bytes_written = self.file.write(data)
        if bytes_written:
            self._throttle(bytes_written)
        return bytes_written

    def _throttle(self, bytes_transferred: int) -> None:
        """Internal method to enforce the rate limit.

        Calculates the time required to transfer the given number of bytes at
        the specified rate and sleeps if the actual transfer was faster.

        Args:
            bytes_transferred: The number of bytes that were just transferred.
        """
        if self.max_bytes_per_sec == float('inf'):
            return

        self.bytes_since_last += bytes_transferred
        elapsed = time.time() - self.last_time

        if elapsed > 0:
            required_time = self.bytes_since_last / self.max_bytes_per_sec
            sleep_time = required_time - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

        if elapsed >= 1.0:
            self.last_time = time.time()
            self.bytes_since_last = 0

    def __getattr__(self, attr: str) -> Any:
        """Proxies any other attribute access to the wrapped file object."""
        return getattr(self.file, attr)


class TransferCheckpoint:
    """Manages a checkpoint file to track completed and failed torrents.

    This class prevents the script from re-processing torrents that have already
    been successfully transferred or that have consistently failed the recheck
    on the destination.

    Attributes:
        file: The `Path` object for the checkpoint JSON file.
        state: A dictionary holding the lists of 'completed' and 'recheck_failed'
               torrent hashes.
    """
    def __init__(self, checkpoint_file: Path):
        """Initializes the TransferCheckpoint.

        Args:
            checkpoint_file: The path to the JSON file used for checkpointing.
        """
        self.file = checkpoint_file
        self.state = self._load()

    def _load(self) -> Dict[str, List[str]]:
        """Loads the state from the checkpoint file.

        Returns:
            The loaded state dictionary. If the file doesn't exist or is corrupt,
            returns a new, empty state.
        """
        if self.file.exists():
            try:
                data = json.loads(self.file.read_text())
                # For backwards compatibility, add recheck_failed if not present
                if "recheck_failed" not in data:
                    data["recheck_failed"] = []
                return data
            except json.JSONDecodeError:
                logging.warning(f"Could not decode checkpoint file '{self.file}'. Starting fresh.")
        return {"completed": [], "recheck_failed": []}

    def mark_completed(self, torrent_hash: str) -> None:
        """Marks a torrent as successfully completed and saves the state.

        Args:
            torrent_hash: The hash of the completed torrent.
        """
        if torrent_hash not in self.state["completed"]:
            self.state["completed"].append(torrent_hash)
            self.clear_recheck_failed(torrent_hash)  # Ensure it's not in the failed list
            self._save()

    def is_completed(self, torrent_hash: str) -> bool:
        """Checks if a torrent is marked as completed.

        Args:
            torrent_hash: The hash of the torrent to check.

        Returns:
            True if the torrent is in the 'completed' list, False otherwise.
        """
        return torrent_hash in self.state["completed"]

    def mark_recheck_failed(self, torrent_hash: str) -> None:
        """Marks a torrent as having failed the recheck on the destination.

        Args:
            torrent_hash: The hash of the failed torrent.
        """
        if torrent_hash not in self.state["recheck_failed"]:
            self.state["recheck_failed"].append(torrent_hash)
        # Also remove it from completed if it's there
        if torrent_hash in self.state["completed"]:
            self.state["completed"].remove(torrent_hash)
        self._save()

    def is_recheck_failed(self, torrent_hash: str) -> bool:
        """Checks if a torrent is in the 'recheck_failed' list.

        Args:
            torrent_hash: The hash of the torrent to check.

        Returns:
            True if the torrent is in the 'recheck_failed' list, False otherwise.
        """
        return torrent_hash in self.state["recheck_failed"]

    def clear_recheck_failed(self, torrent_hash: str) -> None:
        """Removes a torrent from the 'recheck_failed' list.

        Args:
            torrent_hash: The hash of the torrent to clear.
        """
        if torrent_hash in self.state["recheck_failed"]:
            self.state["recheck_failed"].remove(torrent_hash)
            self._save()

    def _save(self) -> None:
        """Saves the current state to the checkpoint file."""
        try:
            self.file.write_text(json.dumps(self.state, indent=2))
        except IOError as e:
            logging.error(f"Failed to save checkpoint file '{self.file}': {e}")


class FileTransferTracker:
    """Tracks the progress of individual file transfers for resumability.

    This class maintains a JSON file that stores the number of bytes transferred
    for each file within a torrent. This allows downloads and uploads to be
    resumed from the point of failure.

    Attributes:
        file: The `Path` object for the tracker JSON file.
        state: The dictionary holding the progress for each file.
    """

    def __init__(self, checkpoint_file: Path):
        """Initializes the FileTransferTracker.

        Args:
            checkpoint_file: The path to the JSON file for tracking progress.
        """
        self.file = checkpoint_file
        self.state = self._load()
        self._lock = threading.Lock()

    def _load(self) -> Dict[str, Any]:
        """Loads the tracker state from its JSON file."""
        if self.file.exists():
            try:
                return json.loads(self.file.read_text())
            except json.JSONDecodeError:
                logging.warning(f"Could not decode file transfer tracker file '{self.file}'. Starting fresh.")
                return {"files": {}}
        return {"files": {}}

    def record_file_progress(self, torrent_hash: str, file_path: str, bytes_transferred: int) -> None:
        """Records the number of bytes transferred for a specific file.

        Args:
            torrent_hash: The hash of the torrent the file belongs to.
            file_path: The path of the file being transferred.
            bytes_transferred: The total number of bytes transferred so far.
        """
        with self._lock:
            key = f"{torrent_hash}:{file_path}"
            self.state["files"][key] = {
                "bytes": bytes_transferred,
                "timestamp": time.time()
            }
            self._save()

    def get_file_progress(self, torrent_hash: str, file_path: str) -> int:
        """Retrieves the last known progress for a file.

        Args:
            torrent_hash: The hash of the torrent.
            file_path: The path of the file.

        Returns:
            The number of bytes previously transferred, or 0 if none.
        """
        with self._lock:
            key = f"{torrent_hash}:{file_path}"
            return self.state["files"].get(key, {}).get("bytes", 0)

    def _save(self) -> None:
        """Saves the tracker state to disk, throttled to reduce I/O."""
        try:
            # Throttle saves to once every 5 seconds to reduce I/O
            if not hasattr(self, "_last_save_time"):
                 self._last_save_time = 0 # Initialize if it doesn't exist

            if time.time() - self._last_save_time > 5:
                self.file.write_text(json.dumps(self.state, indent=2))
                self._last_save_time = time.time()
        except IOError as e:
            logging.error(f"Failed to save file transfer tracker file '{self.file}': {e}")

# --- Constants ---
MAX_RETRY_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 5
GB_BYTES = 1024**3

class Timeouts:
    """Centralizes timeout constants for various network operations."""
    SSH_CONNECT = int(os.getenv('TM_SSH_CONNECT_TIMEOUT', '10'))
    SSH_EXEC = int(os.getenv('TM_SSH_EXEC_TIMEOUT', '60'))
    SFTP_TRANSFER = int(os.getenv('TM_SFTP_TIMEOUT', '300'))
    RECHECK = int(os.getenv('TM_RECHECK_TIMEOUT', '900'))
    POOL_WAIT = int(os.getenv('TM_POOL_WAIT_TIMEOUT', '120'))

def _get_ssh_command(port: int) -> str:
    """Builds the SSH command for rsync with performance optimizations.

    Args:
        port: The SSH port to use.

    Returns:
        A string containing the formatted SSH command.
    """
    base_ssh_cmd = f"ssh -p {port} -c aes128-ctr -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=15"
    if SSH_CONTROL_PATH:
        multiplex_opts = f"-o ControlMaster=auto -o ControlPath={shlex.quote(SSH_CONTROL_PATH)} -o ControlPersist=60s"
        return f"{base_ssh_cmd} {multiplex_opts}"
    return base_ssh_cmd

@retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
def _sftp_download_to_cache(source_pool: SSHConnectionPool, source_file_path: str, local_cache_path: Path, torrent_hash: str, ui: UIManager, file_tracker: FileTransferTracker, download_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """Downloads a file via SFTP to a local cache directory.

    This is the first phase of a cached 'sftp_upload' transfer. It supports
    resuming partial downloads.

    Args:
        source_pool: The SSH connection pool for the source server.
        source_file_path: The absolute path of the file on the source server.
        local_cache_path: The local `Path` object where the file will be cached.
        torrent_hash: The hash of the parent torrent.
        ui: The UIManager instance for progress reporting.
        file_tracker: The tracker for managing resumable transfers.
        download_limit_bytes_per_sec: Bandwidth limit in B/s.
        sftp_chunk_size: The size of data chunks for SFTP reads in bytes.

    Raises:
        Exception: Propagates exceptions from the transfer process on failure.
    """
    try:
        ui.start_file_transfer(torrent_hash, source_file_path, "downloading")
        with source_pool.get_connection() as (sftp, ssh):
            remote_stat = sftp.stat(source_file_path)
            total_size = remote_stat.st_size
            # ui.update_file_status(torrent_hash, source_file_path, "Downloading")
            local_size = 0
            if local_cache_path.exists():
                try:
                    local_size = local_cache_path.stat().st_size
                except FileNotFoundError:
                    local_size = 0
            else:
                file_tracker.record_file_progress(torrent_hash, source_file_path, 0)

            if local_size == total_size:
                logging.info(f"Skipping (exists and size matches): {os.path.basename(source_file_path)}")
                ui.update_torrent_progress(torrent_hash, total_size - local_size, transfer_type='download')
                return
            elif local_size > total_size:
                logging.warning(f"Local file '{os.path.basename(source_file_path)}' is larger than remote ({local_size} > {total_size}), re-downloading from scratch.")
                local_size = 0
            elif local_size > 0:
                logging.info(f"Resuming download for {os.path.basename(source_file_path)} from {local_size / (1024*1024):.2f} MB.")

            mode = 'ab' if local_size > 0 else 'wb'
            with sftp.open(source_file_path, 'rb') as remote_f_raw:
                remote_f_raw.seek(local_size)
                remote_f_raw.prefetch()
                remote_f = RateLimitedFile(remote_f_raw, download_limit_bytes_per_sec)
                with open(local_cache_path, mode) as local_f:
                    while True:
                        chunk = remote_f.read(sftp_chunk_size)
                        if not chunk: break
                        local_f.write(chunk)
                        increment = len(chunk)
                        local_size += increment
                        ui.update_torrent_progress(torrent_hash, increment, transfer_type='download')
                        # ui.advance_overall_progress(increment)
                        file_tracker.record_file_progress(torrent_hash, source_file_path, local_size)
        ui.complete_file_transfer(torrent_hash, source_file_path)
    except Exception as e:
        logging.error(f"Transfer failed for file '{source_file_path}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, source_file_path)
        logging.error(f"Failed to download to cache for {source_file_path}: {e}")
        raise

@retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
def _sftp_upload_from_cache(dest_pool: SSHConnectionPool, local_cache_path: Path, source_file_path: str, dest_file_path: str, torrent_hash: str, ui: UIManager, file_tracker: FileTransferTracker, upload_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """Uploads a file from the local cache to the destination SFTP server.

    This is the second phase of a cached 'sftp_upload' transfer.

    Args:
        dest_pool: The SSH connection pool for the destination server.
        local_cache_path: The local `Path` of the cached file to upload.
        source_file_path: The original source path, used for UI identification.
        dest_file_path: The absolute path on the destination server.
        torrent_hash: The hash of the parent torrent.
        ui: The UIManager instance for progress reporting.
        file_tracker: The tracker for managing resumable transfers.
        upload_limit_bytes_per_sec: Bandwidth limit in B/s.
        sftp_chunk_size: The size of data chunks for SFTP writes in bytes.

    Raises:
        FileNotFoundError: If the local cache file does not exist.
        Exception: Propagates exceptions from the transfer process on failure.
    """
    file_name = local_cache_path.name
    if not local_cache_path.is_file():
        logging.error(f"Cannot upload '{file_name}' from cache: File does not exist.")
        # ui.update_file_status(torrent_hash, source_file_path, "[red]Cache file missing[/red]")
        raise FileNotFoundError(f"Local cache file not found: {local_cache_path}")
    try:
        ui.start_file_transfer(torrent_hash, source_file_path, "uploading")
        total_size = local_cache_path.stat().st_size
        # ui.update_file_status(torrent_hash, source_file_path, "Uploading")
        with dest_pool.get_connection() as (sftp, ssh):
            dest_size = 0
            try:
                dest_size = sftp.stat(dest_file_path).st_size
            except FileNotFoundError:
                pass
            if dest_size >= total_size:
                logging.info(f"Skipping upload (exists and size matches): {file_name}")
                ui.update_torrent_progress(torrent_hash, total_size, transfer_type='upload')
                # ui.advance_overall_progress(total_size)
                return
            if dest_size > 0:
                ui.update_torrent_progress(torrent_hash, dest_size, transfer_type='upload')
                # ui.advance_overall_progress(dest_size)
            dest_dir = os.path.dirname(dest_file_path)
            sftp_mkdir_p(sftp, dest_dir)
            with open(local_cache_path, 'rb') as source_f_raw:
                source_f_raw.seek(dest_size)
                source_f = RateLimitedFile(source_f_raw, upload_limit_bytes_per_sec)
                with sftp.open(dest_file_path, 'ab' if dest_size > 0 else 'wb') as dest_f:
                    while True:
                        chunk = source_f.read(sftp_chunk_size)
                        if not chunk: break
                        dest_f.write(chunk)
                        increment = len(chunk)
                        ui.update_torrent_progress(torrent_hash, increment, transfer_type='upload')
                        # ui.advance_overall_progress(increment)
            if sftp.stat(dest_file_path).st_size != total_size:
                raise Exception("Final size mismatch during cached upload.")
            # ui.update_file_status(torrent_hash, source_file_path, "[green]Completed[/green]")
        ui.complete_file_transfer(torrent_hash, source_file_path)
    except Exception as e:
        logging.error(f"Transfer failed for file '{source_file_path}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, source_file_path)
        # ui.update_file_status(torrent_hash, source_file_path, "[red]Upload Failed[/red]")
        logging.error(f"Upload from cache failed for {file_name}: {e}")
        raise

def _sftp_upload_file(source_pool: SSHConnectionPool, dest_pool: SSHConnectionPool, source_file_path: str, dest_file_path: str, torrent_hash: str, ui: UIManager, file_tracker: FileTransferTracker, dry_run: bool = False, download_limit_bytes_per_sec: int = 0, upload_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """Streams a file directly from a source SFTP server to a destination SFTP server.

    This function is used for `sftp_upload` mode when local caching is disabled.
    It reads chunks from the source and writes them to the destination without
    storing the entire file on the machine running the script.

    Args:
        source_pool: The SSH connection pool for the source server.
        dest_pool: The SSH connection pool for the destination server.
        source_file_path: The absolute path of the file on the source server.
        dest_file_path: The absolute path for the file on the destination server.
        torrent_hash: The hash of the parent torrent.
        ui: The UIManager instance for progress reporting.
        file_tracker: The tracker for managing resumable transfers.
        dry_run: If True, simulates the transfer without moving data.
        download_limit_bytes_per_sec: Bandwidth limit for reading from source.
        upload_limit_bytes_per_sec: Bandwidth limit for writing to destination.
        sftp_chunk_size: The size of data chunks for SFTP I/O in bytes.

    Raises:
        Exception: Propagates exceptions on transfer failure.
    """
    file_name = os.path.basename(source_file_path)
    try:
        ui.start_file_transfer(torrent_hash, source_file_path, "uploading")
        with source_pool.get_connection() as (source_sftp, source_ssh), dest_pool.get_connection() as (dest_sftp, dest_ssh):
            start_time = time.time()
            try:
                source_stat = source_sftp.stat(source_file_path)
                total_size = source_stat.st_size
            except FileNotFoundError:
                logging.warning(f"Source file not found, skipping: {source_file_path}")
                return
            if total_size == 0:
                logging.warning(f"Skipping zero-byte source file: {file_name}")
                return
            dest_size = file_tracker.get_file_progress(torrent_hash, source_file_path)
            try:
                remote_dest_size = dest_sftp.stat(dest_file_path).st_size
                if remote_dest_size > dest_size:
                    dest_size = remote_dest_size
                if dest_size == total_size:
                    logging.info(f"Skipping (exists and size matches): {file_name}")
                    ui.update_torrent_progress(torrent_hash, total_size - dest_size, transfer_type='upload')
                    # ui.advance_overall_progress(total_size - dest_size)
                    return
                elif dest_size > total_size:
                    logging.warning(f"Destination file '{file_name}' is larger than source ({dest_size} > {total_size}). Re-uploading.")
                    dest_size = 0
                else:
                    logging.info(f"Resuming upload for {file_name} from {dest_size / (1024*1024):.2f} MB.")
            except FileNotFoundError:
                pass
            if dest_size > 0:
                ui.update_torrent_progress(torrent_hash, dest_size, transfer_type='upload')
                # ui.advance_overall_progress(dest_size)
            if dry_run:
                logging.info(f"[DRY RUN] Would upload: {source_file_path} -> {dest_file_path}")
                remaining_size = total_size - dest_size
                ui.update_torrent_progress(torrent_hash, remaining_size, transfer_type='upload')
                # ui.advance_overall_progress(remaining_size)
                return
            dest_dir = os.path.dirname(dest_file_path)
            sftp_mkdir_p(dest_sftp, dest_dir)
            with source_sftp.open(source_file_path, 'rb') as source_f_raw:
                source_f_raw.seek(dest_size)
                source_f_raw.prefetch()
                source_f = RateLimitedFile(source_f_raw, download_limit_bytes_per_sec)
                with dest_sftp.open(dest_file_path, 'ab' if dest_size > 0 else 'wb') as dest_f_raw:
                    dest_f = RateLimitedFile(dest_f_raw, upload_limit_bytes_per_sec)
                    while True:
                        chunk = source_f.read(sftp_chunk_size)
                        if not chunk: break
                        dest_f.write(chunk)
                        increment = len(chunk)
                        dest_size += increment
                        ui.update_torrent_progress(torrent_hash, increment, transfer_type='upload')
                        # ui.advance_overall_progress(increment)
                        file_tracker.record_file_progress(torrent_hash, source_file_path, dest_size)
            final_dest_size = dest_sftp.stat(dest_file_path).st_size
            if final_dest_size == total_size:
                end_time = time.time()
                duration = end_time - start_time
                logging.debug(f"Upload of '{file_name}' completed.")
                if duration > 0:
                    speed_mbps = (total_size * 8) / (duration * 1024 * 1024)
                    logging.debug(f"PERF: '{file_name}' ({total_size / 1024**2:.2f} MiB) took {duration:.2f} seconds. Average speed: {speed_mbps:.2f} Mbps.")
                else:
                    logging.debug(f"PERF: '{file_name}' completed in < 1 second.")
            else:
                raise Exception(f"Final size mismatch for {file_name}. Expected {total_size}, got {final_dest_size}")
        ui.complete_file_transfer(torrent_hash, source_file_path)
    except PermissionError as e:
        logging.error(f"Transfer failed for file '{source_file_path}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, source_file_path)
        logging.error(f"Permission denied on destination server for path: {dest_file_path}\n"
                      "Please check that the destination user has write access to that directory.")
        raise e
    except FileNotFoundError as e:
        logging.error(f"Transfer failed for file '{source_file_path}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, source_file_path)
        logging.error(f"Source file not found: {source_file_path}")
        logging.error("This can happen if the file was moved or deleted on the source before transfer.")
        raise e
    except (socket.timeout, TimeoutError) as e:
        logging.error(f"Transfer failed for file '{source_file_path}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, source_file_path)
        logging.error(f"Network timeout during upload of file: {file_name}")
        logging.error("The script will retry, but check your network stability if this persists.")
        raise e
    except Exception as e:
        logging.error(f"Transfer failed for file '{source_file_path}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, source_file_path)
        logging.error(f"Upload failed for {file_name}: {e}")
        raise

@retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
def _sftp_download_file(pool: SSHConnectionPool, remote_file: str, local_file: str, torrent_hash: str, ui: UIManager, file_tracker: FileTransferTracker, dry_run: bool = False, download_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """Downloads a single file from an SFTP server to a local path.

    This function handles resumable downloads, progress reporting, and is wrapped
    with a retry decorator to handle transient network issues.

    Args:
        pool: The SSH connection pool for the source server.
        remote_file: The absolute path of the file on the source SFTP server.
        local_file: The absolute local path where the file will be saved.
        torrent_hash: The hash of the parent torrent.
        ui: The UIManager instance for progress reporting.
        file_tracker: The tracker for managing resumable transfers.
        dry_run: If True, simulates the transfer without moving data.
        download_limit_bytes_per_sec: Bandwidth limit in B/s.
        sftp_chunk_size: The size of data chunks for SFTP reads in bytes.

    Raises:
        Exception: Propagates exceptions on transfer failure.
    """
    local_path = Path(local_file)
    file_name = os.path.basename(remote_file)
    try:
        ui.start_file_transfer(torrent_hash, remote_file, "downloading")
        with pool.get_connection() as (sftp, ssh_client):
            remote_stat = sftp.stat(remote_file)
            total_size = remote_stat.st_size
            logging.debug(f"SFTP Check: Remote file '{remote_file}' size: {total_size}")
            if total_size == 0:
                logging.warning(f"Skipping zero-byte file: {file_name}")
                return
            local_size = 0
            if local_path.exists():
                try:
                    local_size = local_path.stat().st_size
                except FileNotFoundError:
                    local_size = 0
            else:
                file_tracker.record_file_progress(torrent_hash, remote_file, 0)
            if local_size == total_size:
                logging.info(f"Skipping (exists and size matches): {file_name}")
                ui.update_torrent_progress(torrent_hash, total_size - local_size, transfer_type='download')
                return
            elif local_size > total_size:
                logging.warning(f"Local file '{file_name}' is larger than remote ({local_size} > {total_size}), re-downloading from scratch.")
                local_size = 0
            elif local_size > 0:
                logging.info(f"Resuming download for {file_name} from {local_size / (1024*1024):.2f} MB.")

            if local_size > 0:
                ui.update_torrent_progress(torrent_hash, local_size, transfer_type='download')
                # ui.advance_overall_progress(local_size)
            if dry_run:
                logging.info(f"[DRY RUN] Would download: {remote_file} -> {local_path}")
                remaining_size = total_size - local_size
                ui.update_torrent_progress(torrent_hash, remaining_size, transfer_type='download')
                # ui.advance_overall_progress(remaining_size)
                return
            local_path.parent.mkdir(parents=True, exist_ok=True)
            mode = 'ab' if local_size > 0 else 'wb'
            with sftp.open(remote_file, 'rb') as remote_f_raw:
                remote_f_raw.seek(local_size)
                remote_f_raw.prefetch()
                remote_f = RateLimitedFile(remote_f_raw, download_limit_bytes_per_sec)
                with open(local_path, mode) as local_f:
                    while True:
                        chunk = remote_f.read(sftp_chunk_size)
                        if not chunk: break
                        local_f.write(chunk)
                        increment = len(chunk)
                        local_size += increment
                        ui.update_torrent_progress(torrent_hash, increment, transfer_type='download')
                        # ui.advance_overall_progress(increment)
                        file_tracker.record_file_progress(torrent_hash, remote_file, local_size)
            final_local_size = local_path.stat().st_size
            if final_local_size != total_size:
                raise Exception(f"Final size mismatch for {file_name}. Expected {total_size}, got {final_local_size}")
        ui.complete_file_transfer(torrent_hash, remote_file)
    except PermissionError as e:
        logging.error(f"Transfer failed for file '{remote_file}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, remote_file)
        logging.error(f"Permission denied while trying to write to local path: {local_path.parent}\n"
                      "Please check that the user running the script has write permissions for this directory.\n"
                      "If you intended to transfer to another remote server, use 'transfer_mode = sftp_upload' in your config.")
        raise e
    except FileNotFoundError as e:
        logging.error(f"Transfer failed for file '{remote_file}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, remote_file)
        logging.error(f"Source file not found: {remote_file}")
        logging.error("This can happen if the file was moved or deleted on the source before transfer.")
        raise e
    except (socket.timeout, TimeoutError) as e:
        logging.error(f"Transfer failed for file '{remote_file}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, remote_file)
        logging.error(f"Network timeout during download of file: {file_name}")
        logging.error("The script will retry, but check your network stability if this persists.")
        raise e
    except Exception as e:
        logging.error(f"Transfer failed for file '{remote_file}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, remote_file)
        logging.error(f"Download failed for {file_name}: {e}")
        raise

def transfer_content_rsync(sftp_config: configparser.SectionProxy, remote_path: str, local_path: str, torrent_hash: str, ui: UIManager, dry_run: bool = False) -> None:
    """Transfers content from a remote server to a local path using rsync.

    This function constructs and executes an `rsync` command via `sshpass` for
    password authentication. It parses the progress output from `rsync` to
    update the UI in real-time.

    Args:
        sftp_config: The config section for the source server.
        remote_path: The absolute path of the source file or directory.
        local_path: The absolute local path of the destination.
        torrent_hash: The hash of the parent torrent.
        ui: The UIManager instance for progress reporting.
        dry_run: If True, simulates the transfer without moving data.

    Raises:
        Exception: If the rsync command fails after multiple retries.
    """
    # For rsync, we treat the whole torrent as one "file"
    # The file_path is the source_content_path
    rsync_file_name = os.path.basename(remote_path)
    ui.start_file_transfer(torrent_hash, rsync_file_name, "downloading")
    host = sftp_config['host']
    port = sftp_config.getint('port')
    username = sftp_config['username']
    password = sftp_config['password']
    local_parent_dir = os.path.dirname(local_path)
    Path(local_parent_dir).mkdir(parents=True, exist_ok=True)
    remote_spec = f"{username}@{host}:{shlex.quote(remote_path)}"
    rsync_cmd = [
        "sshpass", "-p", password,
        "rsync",
        "-a", "--partial", "--inplace",
        "--info=progress2",
        f"--timeout={Timeouts.SSH_EXEC}",
        "-e", _get_ssh_command(port),
        remote_spec,
        local_parent_dir
    ]
    safe_rsync_cmd = list(rsync_cmd)
    safe_rsync_cmd[2] = "'********'"

    # Get total size from the UI's internal state
    total_size = 0
    with ui._lock:
        if torrent_hash in ui._torrents:
            total_size = ui._torrents[torrent_hash].get("size", 0)

    if dry_run:
        logging.info(f"[DRY RUN] Would execute rsync for: {os.path.basename(remote_path)}")
        logging.debug(f"[DRY RUN] Command: {' '.join(safe_rsync_cmd)}")
        if total_size > 0:
            ui.update_torrent_progress(torrent_hash, total_size, transfer_type='download')
        return
    if Path(local_path).exists():
        logging.info(f"Partial file/directory found for '{os.path.basename(remote_path)}'. Resuming with rsync.")
    else:
        logging.info(f"Starting rsync transfer for '{os.path.basename(remote_path)}'")
    logging.debug(f"Executing rsync command: {' '.join(safe_rsync_cmd)}")
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        if attempt > 1:
            logging.info(f"Rsync attempt {attempt}/{MAX_RETRY_ATTEMPTS} for '{os.path.basename(remote_path)}'...")
            time.sleep(RETRY_DELAY_SECONDS)
        process = None
        try:
            start_time = time.time()
            process = subprocess.Popen(
                rsync_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1
            )
            last_total_transferred = 0
            progress_regex = re.compile(r"^\s*([\d,]+)\s+\d{1,3}%.*$")
            if process.stdout:
                for line in iter(process.stdout.readline, ''):
                    line = line.strip()
                    match = progress_regex.match(line)
                    if match:
                        try:
                            total_transferred_str = match.group(1).replace(',', '')
                            total_transferred = int(total_transferred_str)
                            advance = total_transferred - last_total_transferred
                            if advance > 0:
                                ui.update_torrent_progress(torrent_hash, advance, transfer_type='download')
                                last_total_transferred = total_transferred
                        except (ValueError, IndexError):
                            logging.warning(f"Could not parse rsync progress line: {line}")
                    else:
                        logging.debug(f"rsync stdout: {line}")
            process.wait()
            stderr_output = process.stderr.read() if process.stderr else ""
            if process.returncode == 0 or process.returncode == 24:
                # Ensure the progress bar completes
                if total_size > 0 and last_total_transferred < total_size:
                    remaining = total_size - last_total_transferred
                    ui.update_torrent_progress(torrent_hash, remaining, transfer_type='download')

                logging.info(f"Rsync transfer completed successfully for '{os.path.basename(remote_path)}'.")
                ui.log(f"[green]Rsync complete: {os.path.basename(remote_path)}[/green]")
                ui.complete_file_transfer(torrent_hash, rsync_file_name)
                return
            elif process.returncode == 30:
                logging.warning(f"Rsync timed out for '{os.path.basename(remote_path)}'. Retrying...")
                continue
            else:
                if "permission denied" in stderr_output.lower():
                    logging.error(f"Rsync failed due to a permission error on the local machine.\n"
                                  f"Please check that the user running the script has write permissions for the destination path: {local_parent_dir}")
                else:
                    logging.error(f"Rsync failed for '{os.path.basename(remote_path)}' with non-retryable exit code {process.returncode}.\n"
                                  f"Rsync stderr: {stderr_output}")
                ui.log(f"[bold red]Rsync FAILED for {os.path.basename(remote_path)}[/bold red]")
                logging.error(f"Transfer failed for file '{rsync_file_name}' due to rsync error", exc_info=True)
                ui.fail_file_transfer(torrent_hash, rsync_file_name)
                raise Exception(f"Rsync transfer failed for {os.path.basename(remote_path)}")
        except FileNotFoundError as e:
            logging.error("FATAL: 'rsync' or 'sshpass' command not found.")
            logging.error(f"Transfer failed for file '{rsync_file_name}' due to missing command: {e}", exc_info=True)
            ui.fail_file_transfer(torrent_hash, rsync_file_name)
            raise
        except Exception as e:
            logging.error(f"An exception occurred during rsync for '{os.path.basename(remote_path)}': {e}", exc_info=True)
            if process:
                process.kill()
            if attempt < MAX_RETRY_ATTEMPTS:
                logging.warning("Retrying...")
                continue
            else:
                ui.fail_file_transfer(torrent_hash, rsync_file_name)
                raise e
    logging.error(f"Transfer failed for file '{rsync_file_name}' after multiple retries.", exc_info=True)
    ui.fail_file_transfer(torrent_hash, rsync_file_name)
    raise Exception(f"Rsync transfer for '{os.path.basename(remote_path)}' failed after {MAX_RETRY_ATTEMPTS} attempts.")

def transfer_content(pool: SSHConnectionPool, all_files: List[tuple[str, str]], torrent_hash: str, ui: UIManager, file_tracker: FileTransferTracker, max_concurrent_downloads: int, dry_run: bool = False, download_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """Orchestrates concurrent SFTP downloads for a list of files.

    Uses a `ThreadPoolExecutor` to download multiple files in parallel.

    Args:
        pool: The SSH connection pool for the source server.
        all_files: A list of `(source_path, destination_path)` tuples.
        torrent_hash: The hash of the parent torrent.
        ui: The UIManager instance for progress reporting.
        file_tracker: The tracker for managing resumable transfers.
        max_concurrent_downloads: The maximum number of files to download at once.
        dry_run: If True, simulates the transfers.
        download_limit_bytes_per_sec: Bandwidth limit in B/s.
        sftp_chunk_size: The size of data chunks for SFTP reads in bytes.
    """
    with ThreadPoolExecutor(max_workers=max_concurrent_downloads) as file_executor:
        futures = [
            file_executor.submit(_sftp_download_file, pool, remote_f, local_f, torrent_hash, ui, file_tracker, dry_run, download_limit_bytes_per_sec, sftp_chunk_size)
            for remote_f, local_f in all_files
        ]
        for future in as_completed(futures):
            future.result()

def transfer_content_sftp_upload(
    source_pool: SSHConnectionPool, dest_pool: SSHConnectionPool, all_files: List[tuple[str, str]], torrent_hash: str, ui: UIManager,
    file_tracker: FileTransferTracker, max_concurrent_downloads: int, max_concurrent_uploads: int, total_size: int, dry_run: bool = False, local_cache_sftp_upload: bool = False,
    download_limit_bytes_per_sec: int = 0, upload_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536
) -> None:
    """Orchestrates an SFTP-to-SFTP transfer for a list of files.

    This function can operate in two modes:
    1.  Direct streaming: Reads from the source and writes to the destination
        concurrently (for smaller torrents).
    2.  Local caching: Downloads all files to a temporary local directory first,
        then uploads them to the destination. This is automatically enabled for
        large torrents to conserve memory.

    Args:
        source_pool: The SSH connection pool for the source server.
        dest_pool: The SSH connection pool for the destination server.
        all_files: A list of `(source_path, destination_path)` tuples.
        torrent_hash: The hash of the parent torrent.
        ui: The UIManager instance for progress reporting.
        file_tracker: The tracker for managing resumable transfers.
        max_concurrent_downloads: Max parallel downloads for cached mode.
        max_concurrent_uploads: Max parallel uploads for all modes.
        total_size: The total size of the torrent in bytes.
        dry_run: If True, simulates the transfer.
        local_cache_sftp_upload: If True, forces the use of local caching.
        download_limit_bytes_per_sec: Bandwidth limit for downloads/reads.
        upload_limit_bytes_per_sec: Bandwidth limit for uploads/writes.
        sftp_chunk_size: The size of data chunks for SFTP I/O in bytes.
    """
    if total_size > GB_BYTES and not local_cache_sftp_upload:
        logging.warning(f"Total torrent size ({total_size / GB_BYTES:.2f} GB) exceeds 1 GB threshold.")
        logging.warning("Forcing local caching for this transfer to ensure memory safety.")
        local_cache_sftp_upload = True
    if local_cache_sftp_upload:
        if dry_run:
            logging.info(f"[DRY RUN] Would download {len(all_files)} files to cache and then upload.")
            ui.update_torrent_byte_progress(torrent_hash, ui._torrents_data[torrent_hash]["progress_obj"].tasks[0].total)
            ui.advance_overall_progress(ui._torrents_data[torrent_hash]["progress_obj"].tasks[0].total)
            return
        temp_dir = Path(tempfile.gettempdir()) / f"torrent_mover_cache_{torrent_hash}"
        temp_dir.mkdir(exist_ok=True)
        transfer_successful = False
        try:
            with ThreadPoolExecutor(max_workers=max_concurrent_downloads, thread_name_prefix='CacheDownloader') as download_executor, \
                 ThreadPoolExecutor(max_workers=max_concurrent_uploads, thread_name_prefix='CacheUploader') as upload_executor:
                download_futures = {
                    download_executor.submit(
                        _sftp_download_to_cache, source_pool, source_f,
                        temp_dir / os.path.basename(source_f), torrent_hash, ui,
                        file_tracker, download_limit_bytes_per_sec, sftp_chunk_size
                    ): (source_f, dest_f)
                    for source_f, dest_f in all_files
                }
                upload_futures = []
                for download_future in as_completed(download_futures):
                    source_f, dest_f = download_futures[download_future]
                    try:
                        download_future.result()
                        local_cache_path = temp_dir / os.path.basename(source_f)
                        upload_future = upload_executor.submit(
                            _sftp_upload_from_cache, dest_pool, local_cache_path,
                            source_f, dest_f, torrent_hash, ui,
                            file_tracker, upload_limit_bytes_per_sec, sftp_chunk_size
                        )
                        upload_futures.append(upload_future)
                    except Exception as e:
                        logging.error(f"Download of '{os.path.basename(source_f)}' failed, it will not be uploaded. Error: {e}")
                        raise
                for upload_future in as_completed(upload_futures):
                    upload_future.result()
            transfer_successful = True
        finally:
            if transfer_successful:
                shutil.rmtree(temp_dir, ignore_errors=True)
                logging.debug(f"Successfully removed cache directory: {temp_dir}")
    else:
        with ThreadPoolExecutor(max_workers=max_concurrent_uploads) as file_executor:
            futures = [
                file_executor.submit(
                    _sftp_upload_file, source_pool, dest_pool, source_f, dest_f,
                    torrent_hash, ui, file_tracker, dry_run,
                    download_limit_bytes_per_sec, upload_limit_bytes_per_sec, sftp_chunk_size
                )
                for source_f, dest_f in all_files
            ]
            for future in as_completed(futures):
                future.result()
