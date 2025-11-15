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
from ..strategies.transfer_strategies import TransferFile
from ..ui import UIManagerV2 as UIManager
from ..utils import RemoteTransferError, retry
from ..strategies.transfer_strategies import TransferStrategy, SFTPStrategy
from .resilience import ResilientTransferQueue, StallResilienceWatchdog
from ..torrent_mover import Torrent

if typing.TYPE_CHECKING:
    from ..ui import BaseUIManager
    import qbittorrentapi


def _parse_human_readable_bytes(s: str) -> int:
    """
    Parses a human-readable size string (e.g., '49.41M', '1.2G', '1024') into bytes.
    """
    s = s.strip()
    units = {
        "k": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
    }
    # Check for a unit at the end
    unit = s[-1]
    if unit in units:
        try:
            # Value has a unit (e.g., '49.41M')
            value_str = s[:-1]
            value = float(value_str)
            return int(value * units[unit])
        except ValueError:
            # Malformed string, fall back
            return 0
    else:
        try:
            # No unit, just bytes (e.g., '1024')
            return int(s.replace(',', ''))
        except ValueError:
            return 0


logger = logging.getLogger(__name__)


def _is_sshpass_installed() -> bool:
    """Checks if sshpass is installed and available in the system's PATH."""
    return shutil.which("sshpass") is not None

def _create_safe_command_for_logging(command: List[str]) -> List[str]:
    """Creates a copy of a command list with the sshpass password redacted."""
    safe_command = list(command)
    try:
        # Find the index of 'sshpass' and redact the password after the '-p' flag
        sshpass_index = safe_command.index("sshpass")
        if "-p" in safe_command[sshpass_index:]:
            p_index = safe_command.index("-p", sshpass_index)
            if p_index + 1 < len(safe_command):
                safe_command[p_index + 1] = "'********'"
    except ValueError:
        # 'sshpass' not in command, nothing to redact
        pass
    return safe_command

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



def execute_transfer(strategy: "TransferStrategy", torrent: "Torrent", config: configparser.ConfigParser, ui: "BaseUIManager", file_tracker: "FileTransferTracker", all_files: List["TransferFile"]) -> bool:
    """
    Executes a transfer with centralized resilience and strategy execution.
    """
    stall_timeout = config.getint('ADVANCED', 'stall_timeout_seconds', fallback=300)
    watchdog = StallResilienceWatchdog(torrent.hash, stall_timeout)
    watchdog.start(ui)

    try:
        if strategy.supports_parallel():
            max_concurrent_transfers = config.getint('SETTINGS', 'max_concurrent_downloads', fallback=4)
            queue = ResilientTransferQueue(max_retries=5)
            for file in all_files:
                queue.add(file)

            # This is a simplified server_key. For SFTPUpload, this would need to be more complex
            # but for now, we assume a single source for parallel transfers.
            server_key = f"{strategy.pool.host}:{strategy.pool.port}"

            def worker():
                while True:
                    result = queue.get_next(server_key)
                    if not result:
                        stats = queue.get_stats()
                        if stats["pending"] > 0:
                            time.sleep(1)
                            continue
                        break  # No more items to process

                    file_to_transfer, attempt_count = result
                    try:
                        strategy._transfer_file(file_to_transfer, ui, file_tracker)
                        queue.record_success(file_to_transfer, server_key)
                    except Exception as e:
                        should_retry = queue.record_failure(file_to_transfer, server_key, e, attempt_count)
                        if not should_retry:
                            logging.error(f"Transfer failed permanently for {file_to_transfer.source_path}", exc_info=True)
                            # We don't re-raise here to allow other files to continue
                            # The final check on queue stats will determine torrent success/failure

            with ThreadPoolExecutor(max_workers=max_concurrent_transfers) as executor:
                futures = [executor.submit(worker) for _ in range(max_concurrent_transfers)]
                for future in as_completed(futures):
                    future.result() # Propagate exceptions from workers

            if queue.get_stats()["failed"] > 0:
                raise Exception("One or more files failed to transfer after multiple retries.")
        else:
            # For non-parallel strategies, use a simple retry wrapper
            @retry(tries=3, delay=5)
            def transfer_task():
                strategy.execute(all_files, torrent, ui, file_tracker)

            transfer_task()

        watchdog.stop()
        return True

    except Exception as e:
        logging.error(f"Transfer failed for torrent '{torrent.name}': {e}", exc_info=True)
        watchdog.stop()
        return False
