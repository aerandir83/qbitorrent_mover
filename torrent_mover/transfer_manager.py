import configparser
import json
import logging
import os
import re
import shlex
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import paramiko

from .resilient_queue import ResilientTransferQueue
from .ssh_manager import SSHConnectionPool, sftp_mkdir_p, _get_ssh_command
from .transfer_strategies import TransferFile
from .ui import UIManagerV2 as UIManager
from .utils import RemoteTransferError, retry


if typing.TYPE_CHECKING:
    import qbittorrentapi


logger = logging.getLogger(__name__)


def _is_sshpass_installed() -> bool:
    """Checks if sshpass is installed and available in the system's PATH."""
    return shutil.which("sshpass") is not None

class RateLimitedFile:
    """Wraps a file-like object to throttle read and write operations.

    This class acts as a proxy to a file object, inserting delays into read()
    and write() calls to ensure that the data transfer rate does not exceed a
    specified maximum.

    Attributes:
        file: The underlying file-like object (e.g., from `open()` or SFTP).
        max_bytes_per_sec: The maximum desired transfer speed in bytes per second.
    """
    def __init__(self, file_obj: Any, max_bytes_per_sec: float):
        """Initializes the RateLimitedFile wrapper.

        Args:
            file_obj: The file-like object to wrap.
            max_bytes_per_sec: The maximum transfer speed in bytes per second.
                If 0 or None, the limit is infinite (no throttling).
        """
        self.file = file_obj
        self.max_bytes_per_sec = max_bytes_per_sec if max_bytes_per_sec else float('inf')
        self.last_time = time.time()
        self.bytes_since_last = 0

    def read(self, size: int) -> bytes:
        data = self.file.read(size)
        self._throttle(len(data))
        return data

    def write(self, data: bytes) -> int:
        bytes_written = self.file.write(data)
        if bytes_written:
            self._throttle(bytes_written)
        return bytes_written

    def _throttle(self, bytes_transferred: int) -> None:
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
        """Proxy other attributes to the wrapped file object."""
        return getattr(self.file, attr)


class TransferCheckpoint:
    """Manages a checkpoint file to track processed torrents.

    This class prevents the script from re-processing torrents that have
    already been successfully transferred or that have previously failed the
    recheck process on the destination. It maintains a simple JSON file to
    store the hashes of these torrents.

    Attributes:
        file: The Path object for the checkpoint JSON file.
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
        """Checks if a torrent has been marked as completed.

        Args:
            torrent_hash: The torrent hash to check.

        Returns:
            True if the torrent is in the completed list, False otherwise.
        """
        return torrent_hash in self.state["completed"]

    def mark_recheck_failed(self, torrent_hash: str) -> None:
        """Marks a torrent as having failed recheck and saves the state.

        Args:
            torrent_hash: The hash of the torrent that failed recheck.
        """
        if torrent_hash not in self.state["recheck_failed"]:
            self.state["recheck_failed"].append(torrent_hash)
        # Also remove it from completed if it's there
        if torrent_hash in self.state["completed"]:
            self.state["completed"].remove(torrent_hash)
        self._save()

    def is_recheck_failed(self, torrent_hash: str) -> bool:
        """Checks if a torrent has been marked as having failed recheck.

        Args:
            torrent_hash: The torrent hash to check.

        Returns:
            True if the torrent is in the recheck_failed list, False otherwise.
        """
        return torrent_hash in self.state["recheck_failed"]

    def clear_recheck_failed(self, torrent_hash: str) -> None:
        """Removes a torrent from the recheck_failed list.

        This is useful for manually retrying a torrent that previously failed.

        Args:
            torrent_hash: The hash of the torrent to remove from the list.
        """
        if torrent_hash in self.state["recheck_failed"]:
            self.state["recheck_failed"].remove(torrent_hash)
            self._save()

    def _save(self) -> None:
        try:
            self.file.write_text(json.dumps(self.state, indent=2))
        except IOError as e:
            logging.error(f"Failed to save checkpoint file '{self.file}': {e}")


class FileTransferTracker:
    """Tracks the progress of individual file transfers for resumability.

    This class maintains a JSON file that stores the number of bytes transferred
    for each file. This allows the script to resume downloads or uploads from
    the point of interruption, saving bandwidth and time. Writes to the file
    are throttled to reduce I/O load.

    Attributes:
        file: The Path object for the tracker JSON file.
        state: A dictionary holding the progress state for all tracked files.
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
        """Loads the tracker state from disk.

        If the file doesn't exist or is corrupt, it initializes a new state.
        It also ensures backward compatibility by adding new keys if they are missing.

        Returns:
            The loaded or initialized state dictionary.
        """
        if self.file.exists():
            try:
                data = json.loads(self.file.read_text())
                # For backward compatibility, add new keys if missing
                data.setdefault("files", {})
                data.setdefault("cached_files", {})
                data.setdefault("corruption_hashes", {})
                return data
            except json.JSONDecodeError:
                logging.warning(f"Could not decode file transfer tracker file '{self.file}'. Starting fresh.")
        return {"files": {}, "cached_files": {}, "corruption_hashes": {}}


    def record_file_progress(self, torrent_hash: str, file_path: str, bytes_transferred: int) -> None:
        """Records the number of bytes transferred for a specific file.

        This method is thread-safe.

        Args:
            torrent_hash: The hash of the parent torrent.
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
        """Retrieves the last recorded progress for a specific file.

        This method is thread-safe.

        Args:
            torrent_hash: The hash of the parent torrent.
            file_path: The path of the file.

        Returns:
            The number of bytes transferred, or 0 if the file is not being tracked.
        """
        with self._lock:
            key = f"{torrent_hash}:{file_path}"
            return self.state["files"].get(key, {}).get("bytes", 0)

    def record_cache_location(self, torrent_hash: str, file_path: str, cache_path: str, size: int):
        """Stores the location and size of a cached file."""
        with self._lock:
            key = f"{torrent_hash}:{file_path}"
            self.state["cached_files"][key] = {
                "path": cache_path,
                "size": size,
                "timestamp": time.time()
            }
            self._save()

    def get_cache_location(self, torrent_hash: str, file_path: str) -> Optional[str]:
        """Retrieves a valid cache path if it exists and matches the expected size."""
        with self._lock:
            key = f"{torrent_hash}:{file_path}"
            cache_info = self.state["cached_files"].get(key)

            if not cache_info:
                return None

            cache_path_str = cache_info.get("path")
            expected_size = cache_info.get("size")

            if not cache_path_str or expected_size is None:
                del self.state["cached_files"][key]
                self._save()
                return None

            cache_path = Path(cache_path_str)
            if self.verify_file_integrity(cache_path, expected_size):
                return cache_path_str
            else:
                logging.warning(f"Cached file for '{file_path}' is invalid (missing or size mismatch). Removing entry.")
                del self.state["cached_files"][key]
                self._save()
                return None

    def clear_torrent_cache(self, torrent_hash: str):
        """Removes all cache location records for a given torrent."""
        with self._lock:
            keys_to_delete = [
                key for key in self.state.get("cached_files", {})
                if key.startswith(f"{torrent_hash}:")
            ]
            if keys_to_delete:
                for key in keys_to_delete:
                    del self.state["cached_files"][key]
                self._save()
                logging.info(f"Cleared {len(keys_to_delete)} cache records for torrent {torrent_hash}.")

    def record_corruption(self, torrent_hash: str, file_path: str, checksum: Optional[str] = None):
        """Marks a file as corrupted."""
        with self._lock:
            key = f"{torrent_hash}:{file_path}"
            corruption_info = self.state["corruption_hashes"].get(key, {"attempts": 0})
            corruption_info["timestamp"] = time.time()
            corruption_info["attempts"] += 1
            if checksum:
                corruption_info["checksum"] = checksum
            self.state["corruption_hashes"][key] = corruption_info
            self._save()
            logging.warning(f"Recorded corruption for file '{file_path}' (Attempt {corruption_info['attempts']}).")

    def is_corrupted(self, torrent_hash: str, file_path: str) -> bool:
        """Checks if a file is marked as corrupted and shouldn't be retried."""
        with self._lock:
            key = f"{torrent_hash}:{file_path}"
            corruption_info = self.state["corruption_hashes"].get(key)

            if not corruption_info:
                return False

            # Allow retry after 24 hours
            if time.time() - corruption_info.get("timestamp", 0) > 86400: # 24 * 60 * 60
                logging.info(f"Corruption marker for '{file_path}' has expired. Allowing a new attempt.")
                del self.state["corruption_hashes"][key]
                self._save()
                return False

            # Deny retry if attempts exceed threshold
            if corruption_info.get("attempts", 0) >= 3:
                logging.warning(f"File '{file_path}' is marked as corrupted and has reached max retry attempts.")
                return True

            return False

    def verify_file_integrity(self, file_path: Path, expected_size: int) -> bool:
        """Checks if a file exists and has the expected size."""
        if not file_path.exists():
            return False
        try:
            return file_path.stat().st_size == expected_size
        except FileNotFoundError:
            return False

    def _save(self) -> None:
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
    SSH_CONNECT = int(os.getenv('TM_SSH_CONNECT_TIMEOUT', '10'))
    SSH_EXEC = int(os.getenv('TM_SSH_EXEC_TIMEOUT', '60'))
    SFTP_TRANSFER = int(os.getenv('TM_SFTP_TIMEOUT', '300'))
    RECHECK = int(os.getenv('TM_RECHECK_TIMEOUT', '900'))
    POOL_WAIT = int(os.getenv('TM_POOL_WAIT_TIMEOUT', '120'))

@retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
def _sftp_download_to_cache(source_pool: SSHConnectionPool, source_file_path: str, local_cache_path: Path, torrent_hash: str, ui: UIManager, file_tracker: FileTransferTracker, download_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """Downloads a file via SFTP to a local cache directory.

    This function is used as part of the cached SFTP-to-SFTP transfer mode.
    It supports resuming and progress reporting to the UI.

    Args:
        source_pool: The SSHConnectionPool for the source server.
        source_file_path: The absolute path of the file on the source server.
        local_cache_path: The local path where the file will be cached.
        torrent_hash: The hash of the parent torrent.
        ui: The UI manager for progress updates.
        file_tracker: The tracker for recording file progress.
        download_limit_bytes_per_sec: The download speed limit in bytes per second.
        sftp_chunk_size: The size of data chunks for SFTP transfers.

    Raises:
        Exception: Propagates exceptions from the underlying SFTP operations.
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

            with ui._lock:
                if torrent_hash not in ui._file_progress:
                    ui._file_progress[torrent_hash] = {}
                ui._file_progress[torrent_hash][source_file_path] = (local_size, total_size)
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
                        with ui._lock:
                            if torrent_hash in ui._file_progress:
                                ui._file_progress[torrent_hash][source_file_path] = (local_size, total_size)
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

    This function is the second part of a cached SFTP-to-SFTP transfer. It
    supports resuming and progress reporting.

    Args:
        dest_pool: The SSHConnectionPool for the destination server.
        local_cache_path: The path to the cached file on the local machine.
        source_file_path: The original source path of the file (for UI).
        dest_file_path: The absolute path on the destination server.
        torrent_hash: The hash of the parent torrent.
        ui: The UI manager for progress updates.
        file_tracker: The tracker for recording file progress.
        upload_limit_bytes_per_sec: The upload speed limit in bytes per second.
        sftp_chunk_size: The size of data chunks for SFTP transfers.

    Raises:
        FileNotFoundError: If the local cache file does not exist.
        Exception: Propagates exceptions from the underlying SFTP operations.
    """
    file_name = local_cache_path.name
    if not local_cache_path.is_file():
        logging.error(f"Cannot upload '{file_name}' from cache: File does not exist.")
        # ui.update_file_status(torrent_hash, source_file_path, "[red]Cache file missing[/red]")
        raise FileNotFoundError(f"Local cache file not found: {local_cache_path}")
    try:
        ui.start_file_transfer(torrent_hash, source_file_path, "uploading")
        total_size = local_cache_path.stat().st_size
        with ui._lock:
            if torrent_hash not in ui._file_progress:
                ui._file_progress[torrent_hash] = {}
            ui._file_progress[torrent_hash][source_file_path] = (0, total_size)
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
                        dest_size += increment
                        with ui._lock:
                            if torrent_hash in ui._file_progress:
                                ui._file_progress[torrent_hash][source_file_path] = (dest_size, total_size)
                        ui.update_torrent_progress(torrent_hash, increment, transfer_type='upload')
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

    This function is used for the direct (non-cached) `sftp_upload` mode.
    It opens connections to both servers and transfers the file in chunks,
    avoiding the need to store the entire file in memory or on disk.

    Args:
        source_pool: The SSHConnectionPool for the source server.
        dest_pool: The SSHConnectionPool for the destination server.
        source_file_path: The absolute path of the file on the source server.
        dest_file_path: The absolute path on the destination server.
        torrent_hash: The hash of the parent torrent.
        ui: The UI manager for progress updates.
        file_tracker: The tracker for recording file progress.
        dry_run: If True, simulates the transfer.
        download_limit_bytes_per_sec: Speed limit for reading from the source.
        upload_limit_bytes_per_sec: Speed limit for writing to the destination.
        sftp_chunk_size: The size of data chunks for SFTP transfers.

    Raises:
        Exception: Propagates exceptions from the underlying SFTP operations.
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
            with ui._lock:
                if torrent_hash not in ui._file_progress:
                    ui._file_progress[torrent_hash] = {}
                ui._file_progress[torrent_hash][source_file_path] = (0, total_size)
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
                        with ui._lock:
                            if torrent_hash in ui._file_progress:
                                ui._file_progress[torrent_hash][source_file_path] = (dest_size, total_size)
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

def _sftp_download_file_core(pool: SSHConnectionPool, file: TransferFile, ui: UIManager, file_tracker: FileTransferTracker, dry_run: bool = False, download_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """Downloads a single file from a remote SFTP server to a local path.

    This function is used for the `sftp` transfer mode. It is wrapped in a
    retry decorator and handles its own SFTP session to ensure thread safety.
    It supports resuming and progress reporting.

    Args:
        pool: The SSHConnectionPool for the source server.
        file: The TransferFile object with source, dest, and size.
        ui: The UI manager for progress updates.
        file_tracker: The tracker for recording file progress.
        dry_run: If True, simulates the transfer.
        download_limit_bytes_per_sec: The download speed limit in bytes per second.
        sftp_chunk_size: The size of data chunks for SFTP transfers.

    Raises:
        Exception: Propagates exceptions from the underlying SFTP operations.
    """
    remote_file = file.source_path
    local_file = file.dest_path
    torrent_hash = file.torrent_hash
    local_path = Path(local_file)
    file_name = os.path.basename(remote_file)
    try:
        if file_tracker.is_corrupted(torrent_hash, remote_file):
            raise Exception(f"File {file_name} is marked as corrupted, skipping.")

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
            with ui._lock:
                if torrent_hash not in ui._file_progress:
                    ui._file_progress[torrent_hash] = {}
                ui._file_progress[torrent_hash][remote_file] = (local_size, total_size)
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
                        with ui._lock:
                            if torrent_hash in ui._file_progress:
                                ui._file_progress[torrent_hash][remote_file] = (local_size, total_size)
                        ui.update_torrent_progress(torrent_hash, increment, transfer_type='download')
                        # ui.advance_overall_progress(increment)
                        file_tracker.record_file_progress(torrent_hash, remote_file, local_size)

            if not file_tracker.verify_file_integrity(local_path, total_size):
                file_tracker.record_corruption(torrent_hash, remote_file)
                raise Exception(f"File integrity check failed for {file_name}")
        ui.complete_file_transfer(torrent_hash, remote_file)
    except PermissionError as e:
        file_tracker.record_corruption(torrent_hash, remote_file)
        logging.error(f"Transfer failed for file '{remote_file}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, remote_file)
        logging.error(f"Permission denied while trying to write to local path: {local_path.parent}\n"
                      "Please check that the user running the script has write permissions for this directory.\n"
                      "If you intended to transfer to another remote server, use 'transfer_mode = sftp_upload' in your config.")
        raise e
    except FileNotFoundError as e:
        file_tracker.record_corruption(torrent_hash, remote_file)
        logging.error(f"Transfer failed for file '{remote_file}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, remote_file)
        logging.error(f"Source file not found: {remote_file}")
        logging.error("This can happen if the file was moved or deleted on the source before transfer.")
        raise e
    except (socket.timeout, TimeoutError) as e:
        file_tracker.record_corruption(torrent_hash, remote_file)
        logging.error(f"Transfer failed for file '{remote_file}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, remote_file)
        logging.error(f"Network timeout during download of file: {file_name}")
        logging.error("The script will retry, but check your network stability if this persists.")
        raise e
    except Exception as e:
        file_tracker.record_corruption(torrent_hash, remote_file)
        logging.error(f"Transfer failed for file '{remote_file}' due to error: {e}", exc_info=True)
        ui.fail_file_transfer(torrent_hash, remote_file)
        logging.error(f"Download failed for {file_name}: {e}")
        raise


def _sftp_download_file_resilient(
    pool: SSHConnectionPool,
    file: TransferFile,
    queue: ResilientTransferQueue,
    ui: UIManager,
    file_tracker: FileTransferTracker,
    attempt_count: int,
    server_key: str,
    dry_run: bool = False,
    download_limit_bytes_per_sec: int = 0,
    sftp_chunk_size: int = 65536
) -> None:
    """Wrapper for _sftp_download_file_core that integrates with ResilientTransferQueue."""
    try:
        _sftp_download_file_core(
            pool, file, ui, file_tracker, dry_run,
            download_limit_bytes_per_sec, sftp_chunk_size
        )
        queue.record_success(file, server_key)
    except (socket.timeout, TimeoutError, paramiko.SSHException) as e:
        should_retry = queue.record_failure(file, server_key, e, attempt_count)
        if not should_retry:
            logging.error(f"SFTP download failed permanently for {file.source_path} due to network error: {e}", exc_info=True)
            raise  # Re-raise to signal permanent failure
    except (PermissionError, FileNotFoundError) as e:
        # These are permanent failures, do not retry
        queue.record_failure(file, server_key, e, 999) # 999 ensures it's marked as failed
        logging.error(f"SFTP download failed permanently for {file.source_path} due to file/permission error: {e}", exc_info=True)
        raise
    except Exception as e:
        # For other unexpected errors, treat as potentially transient
        should_retry = queue.record_failure(file, server_key, e, attempt_count)
        if not should_retry:
            logging.error(f"SFTP download failed permanently for {file.source_path} due to unexpected error: {e}", exc_info=True)
            raise


@retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
def _transfer_content_rsync_upload_from_cache(dest_config: configparser.SectionProxy, local_path: str, remote_path: str, torrent_hash: str, ui: UIManager, rsync_options: List[str], dry_run: bool = False) -> None:
    """
    Transfers content from a local path to a remote server using rsync.
    This is the UPLOAD part of the cache-based rsync_upload mode.
    """
    file_name = os.path.basename(local_path)
    ui.start_file_transfer(torrent_hash, file_name, "uploading")

    host = dest_config['host']
    port = dest_config.getint('port')
    username = dest_config['username']
    password = dest_config['password']

    # Ensure the remote *parent* directory exists.
    # rsync will create the final content directory.
    remote_parent_dir = os.path.dirname(remote_path)
    cleaned_remote_parent_dir = remote_parent_dir.strip('\'"')
    # DO NOT use shlex.quote here. The full remote_spec is a single
    # argument for subprocess, so rsync will parse the path correctly.
    remote_spec = f"{username}@{host}:{cleaned_remote_parent_dir}"

    rsync_cmd = [
        "sshpass", "-p", password,
        "rsync",
        *rsync_options,
        "--info=progress2",
        f"--timeout={Timeouts.SSH_EXEC}",
        "-e", _get_ssh_command(port),
        local_path, # Source is local
        remote_spec # Destination is remote
    ]
    safe_rsync_cmd = list(rsync_cmd)
    safe_rsync_cmd[2] = "'********'"

    total_size = 0
    with ui._lock:
        if torrent_hash in ui._torrents:
            # We divide by 2 because this is the second half of a 2x multiplier transfer
            total_size = ui._torrents[torrent_hash].get("size", 0) / 2

    if dry_run:
        logging.info(f"[DRY RUN] Would execute rsync upload for: {file_name}")
        logging.debug(f"[DRY RUN] Command: {' '.join(safe_rsync_cmd)}")
        if total_size > 0:
            ui.update_torrent_progress(torrent_hash, total_size, transfer_type='upload')
        ui.complete_file_transfer(torrent_hash, file_name)
        return

    logging.info(f"Starting rsync upload from cache for '{file_name}'")
    logging.debug(f"Executing rsync upload: {' '.join(safe_rsync_cmd)}")

    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        if attempt > 1:
            logging.info(f"Rsync upload attempt {attempt}/{MAX_RETRY_ATTEMPTS} for '{file_name}'...")
            time.sleep(RETRY_DELAY_SECONDS)

        process = None
        try:
            try:
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
                                    ui.update_torrent_progress(torrent_hash, advance, transfer_type='upload')
                                    last_total_transferred = total_transferred
                            except (ValueError, IndexError):
                                logging.warning(f"Could not parse rsync upload progress line: {line}")

                process.wait()
                stderr_output = process.stderr.read() if process.stderr else ""

                if process.returncode == 0 or process.returncode == 24:
                    if total_size > 0 and last_total_transferred < total_size:
                        remaining = total_size - last_total_transferred
                        ui.update_torrent_progress(torrent_hash, remaining, transfer_type='upload')

                    logging.info(f"Rsync upload from cache completed for '{file_name}'.")
                    ui.log(f"[green]Rsync upload complete: {file_name}[/green]")
                    ui.complete_file_transfer(torrent_hash, file_name)
                    return
                elif process.returncode == 30:
                    logging.warning(f"Rsync upload timed out for '{file_name}'. Retrying...")
                    continue
                else:
                    logging.error(f"Rsync upload failed for '{file_name}' with non-retryable exit code {process.returncode}.\n"
                                  f"Rsync stderr: {stderr_output}")
                    ui.log(f"[bold red]Rsync Upload FAILED for {file_name}[/bold red]")
                    ui.fail_file_transfer(torrent_hash, file_name)
                    raise RemoteTransferError(f"Rsync upload failed (exit {process.returncode}): {stderr_output}")

            except Exception as e:
                logging.error(f"An exception occurred during rsync upload for '{file_name}': {e}", exc_info=True)
                if process:
                    process.kill()
                if attempt < MAX_RETRY_ATTEMPTS:
                    logging.warning("Retrying...")
                    continue
                else:
                    ui.fail_file_transfer(torrent_hash, file_name)
                    raise e

        finally:
            if process and process.poll() is None:
                logging.warning(f"Terminating lingering rsync upload process for '{file_name}' (PID: {process.pid})")
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    logging.warning(f"Rsync upload process {process.pid} did not terminate, killing.")
                except Exception as e:
                    logging.error(f"Error terminating rsync upload process {process.pid}: {e}")

    logging.error(f"Upload from cache failed for '{file_name}' after multiple retries.", exc_info=True)
    ui.fail_file_transfer(torrent_hash, file_name)
    raise RemoteTransferError(f"Rsync upload for '{file_name}' failed after {MAX_RETRY_ATTEMPTS} attempts.")


def transfer_content_rsync(
    sftp_config: configparser.SectionProxy,
    remote_path: str,
    local_path: str,
    torrent_hash: str,
    ui: UIManager,
    rsync_options: List[str],
    file_tracker: FileTransferTracker,  # FIX 2: Change from bool to FileTransferTracker
    dry_run: bool = False
) -> None:
    """Transfers content from a remote server to a local path using rsync."""

    # FIX 2: Now we can properly call is_corrupted on the FileTransferTracker instance
    if file_tracker.is_corrupted(torrent_hash, remote_path):
        raise Exception(f"Transfer for {os.path.basename(remote_path)} is marked as corrupted, skipping.")

    # FIX 1: Strip quotes from the remote path before using it
    remote_path = remote_path.strip('\'"')

    rsync_file_name = os.path.basename(remote_path)
    ui.start_file_transfer(torrent_hash, rsync_file_name, "downloading")

    host = sftp_config['host']
    port = sftp_config.getint('port')
    username = sftp_config['username']
    password = sftp_config['password']

    local_parent_dir = os.path.dirname(local_path)
    Path(local_parent_dir).mkdir(parents=True, exist_ok=True)

    rsync_command_base = [
        "rsync",
        *rsync_options,
        "-e", f"sshpass -p {shlex.quote(password)} ssh -p {port} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
    ]

    remote_spec = f"{username}@{host}:{remote_path}"

    MAX_RETRY_ATTEMPTS = 3
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        if attempt > 1:
            logging.info(f"Rsync attempt {attempt}/{MAX_RETRY_ATTEMPTS} for '{os.path.basename(remote_path)}'...")
            time.sleep(RETRY_DELAY_SECONDS)
        process = None
        try:
            try:
                rsync_command = [*rsync_command_base, remote_spec, local_parent_dir]

                if dry_run:
                    logging.info(f"[DRY_RUN] Would execute: {' '.join(rsync_command)}")
                    ui.complete_file_transfer(torrent_hash, rsync_file_name, 0, "dry_run") # Simulate 0 bytes
                    return

                process = subprocess.Popen(
                    rsync_command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True
                )

                # Monitor stdout for progress
                if process.stdout:
                    # This loop consumes stdout. We only act on "to-check" lines for the UI.
                    # Other lines (like the per-file progress %) are read and discarded,
                    # preventing them from spamming the DEBUG log.
                    for line in iter(process.stdout.readline, ''):
                        if "to-check=" in line or "to-chk" in line:
                            ui.update_file_transfer_progress(torrent_hash, rsync_file_name, line.strip())

                process.wait()

                if process.returncode == 0:
                    total_bytes = os.path.getsize(local_path) if os.path.exists(local_path) else 0
                    ui.complete_file_transfer(torrent_hash, rsync_file_name, total_bytes, "completed")
                    logging.info(f"Rsync transfer completed for {os.path.basename(remote_path)}")
                    return
                else:
                    stderr_output = process.stderr.read() if process.stderr else ""
                    logging.error(f"Rsync transfer failed for '{rsync_file_name}' (Attempt {attempt}/{MAX_RETRY_ATTEMPTS}). Error: {stderr_output}")
                    if "No such file or directory" in stderr_output:
                         # Don't retry if the file doesn't exist
                        raise FileNotFoundError(f"Source file not found: {remote_path}")

            except subprocess.CalledProcessError as e:
                file_tracker.record_corruption(torrent_hash, remote_path)
                logging.error(f"Transfer failed for file '{rsync_file_name}' due to rsync error", exc_info=True)
                ui.fail_file_transfer(torrent_hash, rsync_file_name)
                raise Exception(f"Rsync transfer failed for {os.path.basename(remote_path)}")

            except FileNotFoundError as e:
                file_tracker.record_corruption(torrent_hash, remote_path)
                # This error can be one of two things:
                # 1. The Popen() call failed because 'rsync' or 'sshpass' is not installed locally.
                # 2. The rsync command returned "No such file or directory", and we raised this exception manually.
                logging.error(f"Rsync transfer failed with FileNotFoundError: {e}")
                logging.error(f"This could be a missing command (rsync, sshpass) on this machine, or the source file was not found on the remote server.", exc_info=True)
                ui.fail_file_transfer(torrent_hash, rsync_file_name)
                raise
            except Exception as e:
                file_tracker.record_corruption(torrent_hash, remote_path)
                logging.error(f"An exception occurred during rsync for '{os.path.basename(remote_path)}': {e}", exc_info=True)
                if process:
                    process.kill()
                if attempt < MAX_RETRY_ATTEMPTS:
                    logging.warning("Retrying...")
                    continue
                else:
                    ui.fail_file_transfer(torrent_hash, rsync_file_name)
                    raise e
        finally:
            if process and process.poll() is None:
                logging.warning(f"Terminating lingering rsync process for '{rsync_file_name}' (PID: {process.pid})")
                try:
                    process.terminate()
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    logging.warning(f"Rsync process {process.pid} did not terminate, killing.")
                except Exception as e:
                    logging.error(f"Error terminating rsync process {process.pid}: {e}")

    file_tracker.record_corruption(torrent_hash, remote_path)
    logging.error(f"Transfer failed for file '{rsync_file_name}' after multiple retries.", exc_info=True)
    ui.fail_file_transfer(torrent_hash, rsync_file_name)
    raise Exception(f"Rsync transfer failed for {rsync_file_name} after {MAX_RETRY_ATTEMPTS} attempts")


def transfer_content_rsync_upload(
    source_config: configparser.SectionProxy,
    dest_config: configparser.SectionProxy,
    rsync_options: List[str],
    source_content_path: str,
    dest_content_path: str,
    torrent_hash: str,
    ui: UIManager,
    file_tracker: FileTransferTracker,
    dry_run: bool,
    is_folder: bool
) -> bool:
    """
    Transfers content from a remote source to a remote destination
    by downloading to a local cache via rsync, then uploading
    from the cache to the destination via rsync.
    """
    file_name = os.path.basename(source_content_path)
    temp_dir = Path(tempfile.gettempdir()) / f"torrent_mover_cache_{torrent_hash}"
    temp_dir.mkdir(exist_ok=True)

    # The local path for the *content itself* inside the cache dir
    local_cache_content_path = str(temp_dir / file_name)

    try:
        # --- 1. Download from Source to Cache ---
        logging.info(f"Rsync-Upload: Downloading '{file_name}' to cache...")
        # We pass temp_dir as the local_path, so rsync downloads
        # the content *into* this directory.
        transfer_content_rsync(
            source_config,
            source_content_path,
            local_cache_content_path,
            torrent_hash,
            ui,
            rsync_options,
            file_tracker,
            dry_run
        )
        logging.info(f"Rsync-Upload: Download to cache complete for '{file_name}'.")

        # --- 2. Upload from Cache to Destination ---
        logging.info(f"Rsync-Upload: Uploading '{file_name}' from cache to destination...")

        # We pass the path to the *content* in the cache
        # and the *parent* directory on the destination.
        _transfer_content_rsync_upload_from_cache(
            dest_config,
            local_cache_content_path, # Upload the content
            dest_content_path,
            torrent_hash,
            ui,
            rsync_options,
            dry_run
        )
        logging.info(f"Rsync-Upload: Upload from cache complete for '{file_name}'.")

        # --- 3. Cleanup Cache ---
        if not dry_run:
            logging.debug(f"Cleaning up cache directory: {temp_dir}")
            shutil.rmtree(temp_dir)

        return True

    except Exception as e:
        logging.error(f"Exception during rsync_upload for '{file_name}': {e}", exc_info=True)
        # Don't clean up cache on failure, so it can be resumed
        raise RemoteTransferError(f"Rsync_upload transfer failed for {file_name}") from e


def transfer_content_with_queue(
    pool: SSHConnectionPool,
    all_files: List[TransferFile],
    torrent_hash: str,
    ui: UIManager,
    file_tracker: FileTransferTracker,
    max_concurrent_downloads: int,
    dry_run: bool = False,
    download_limit_bytes_per_sec: int = 0,
    sftp_chunk_size: int = 65536
) -> None:
    """Transfers torrent content using a resilient queue."""
    queue = ResilientTransferQueue(max_retries=5)
    for file in all_files:
        queue.add(file)

    # Simplified server key for now
    server_key = f"{pool.host}:{pool.port}"

    def worker():
        while True:
            result = queue.get_next(server_key)
            if not result:
                # Check if there are still pending items (e.g. in backoff)
                stats = queue.get_stats()
                if stats["pending"] > 0:
                    time.sleep(1) # Wait for items in backoff
                    continue
                break # No more items to process

            file, attempt_count = result
            try:
                _sftp_download_file_resilient(
                    pool, file, queue, ui, file_tracker, attempt_count,
                    server_key, dry_run, download_limit_bytes_per_sec, sftp_chunk_size
                )
            except Exception:
                # Permanent failures are logged in _resilient wrapper
                # And re-raised to stop the executor
                pass # Continue to next file

    with ThreadPoolExecutor(max_workers=max_concurrent_downloads) as executor:
        futures = [executor.submit(worker) for _ in range(max_concurrent_downloads)]
        for future in as_completed(futures):
            # This will re-raise exceptions from workers if any occurred
            future.result()

    final_stats = queue.get_stats()
    if final_stats["failed"] > 0:
        logging.error(f"{final_stats['failed']} files failed to transfer for torrent {torrent_hash}.")
        # Optionally, you can raise an exception to mark the whole torrent failed
        raise RemoteTransferError(f"{final_stats['failed']} files failed for torrent {torrent_hash}")


def transfer_content_sftp_upload(
    source_pool: SSHConnectionPool, dest_pool: SSHConnectionPool, all_files: List[tuple[str, str]], torrent_hash: str, ui: UIManager,
    file_tracker: FileTransferTracker, max_concurrent_downloads: int, max_concurrent_uploads: int, total_size: int, dry_run: bool = False, local_cache_sftp_upload: bool = False,
    download_limit_bytes_per_sec: int = 0, upload_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536
) -> None:
    """Transfers a torrent's content from a source to a destination SFTP server.

    This function orchestrates the `sftp_upload` transfer mode. It can operate
    in two ways:
    1.  Direct Streaming: For smaller torrents, it streams files directly from
        the source to the destination using `_sftp_upload_file`.
    2.  Local Caching: For torrents larger than 1GB or when explicitly enabled,
        it first downloads all files to a local temporary cache directory
        using `_sftp_download_to_cache` and then uploads them to the
        destination using `_sftp_upload_from_cache`.

    Args:
        source_pool: The SSHConnectionPool for the source server.
        dest_pool: The SSHConnectionPool for the destination server.
        all_files: A list of (source_path, dest_path) tuples.
        torrent_hash: The hash of the parent torrent.
        ui: The UI manager for progress updates.
        file_tracker: The tracker for recording file progress.
        max_concurrent_downloads: Max parallel downloads for cached mode.
        max_concurrent_uploads: Max parallel uploads.
        total_size: The total size of the torrent's content in bytes.
        dry_run: If True, simulates the transfer.
        local_cache_sftp_upload: If True, forces the use of local caching.
        download_limit_bytes_per_sec: The download speed limit.
        upload_limit_bytes_per_sec: The upload speed limit.
        sftp_chunk_size: The chunk size for SFTP transfers.
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

                download_futures = {}
                upload_futures = []

                # Phase 1: Check for existing cache and submit downloads/uploads
                for source_f, dest_f in all_files:
                    existing_cache = file_tracker.get_cache_location(torrent_hash, source_f)
                    if existing_cache:
                        logging.info(f"Found valid cache for '{os.path.basename(source_f)}'. Skipping download.")
                        # Advance the download progress bar for the cached file
                        file_size = Path(existing_cache).stat().st_size
                        ui.update_torrent_progress(torrent_hash, file_size, transfer_type='download')
                        # Submit upload directly
                        upload_future = upload_executor.submit(
                            _sftp_upload_from_cache, dest_pool, Path(existing_cache),
                            source_f, dest_f, torrent_hash, ui,
                            file_tracker, upload_limit_bytes_per_sec, sftp_chunk_size
                        )
                        upload_futures.append(upload_future)
                    else:
                        # Submit download
                        future = download_executor.submit(
                            _sftp_download_to_cache, source_pool, source_f,
                            temp_dir / os.path.basename(source_f), torrent_hash, ui,
                            file_tracker, download_limit_bytes_per_sec, sftp_chunk_size
                        )
                        download_futures[future] = (source_f, dest_f)

                # Phase 2: Process completed downloads and submit their uploads
                for download_future in as_completed(download_futures):
                    source_f, dest_f = download_futures[download_future]
                    try:
                        download_future.result() # Propagate exceptions
                        local_cache_path = temp_dir / os.path.basename(source_f)

                        # Record the successful cache location
                        cache_size = local_cache_path.stat().st_size
                        file_tracker.record_cache_location(torrent_hash, source_f, str(local_cache_path), cache_size)

                        # Submit for upload
                        upload_future = upload_executor.submit(
                            _sftp_upload_from_cache, dest_pool, local_cache_path,
                            source_f, dest_f, torrent_hash, ui,
                            file_tracker, upload_limit_bytes_per_sec, sftp_chunk_size
                        )
                        upload_futures.append(upload_future)
                    except Exception as e:
                        logging.error(f"Download of '{os.path.basename(source_f)}' failed, it will not be uploaded. Error: {e}")
                        raise # This will stop the entire transfer

                # Phase 3: Wait for all uploads to complete
                for upload_future in as_completed(upload_futures):
                    upload_future.result() # Propagate exceptions

            transfer_successful = True
        finally:
            if transfer_successful:
                shutil.rmtree(temp_dir, ignore_errors=True)
                file_tracker.clear_torrent_cache(torrent_hash)
                logging.debug(f"Successfully removed cache directory: {temp_dir}")
            else:
                logging.warning(f"Transfer for torrent {torrent_hash} failed. Keeping cache for resume.")
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
