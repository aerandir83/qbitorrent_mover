"""
sftp_manager.py - High-performance SFTP Logic

Encapsulates:
1. SFTP Download Logic (Resuming, Throttling, Verification)
2. Rate Limiting
"""

import os
import time
import logging
from pathlib import Path
from typing import Callable, Any, Optional

import paramiko
from utils import RemoteTransferError

logger = logging.getLogger(__name__)

class RateLimitedFile:
    """
    Wraps a file-like object to throttle read and write operations.
    
    This class acts as a proxy to a file object, inserting delays into read()
    and write() calls to ensure that the data transfer rate does not exceed a
    specified maximum.
    """
    def __init__(self, file_obj: Any, max_bytes_per_sec: float):
        self.file = file_obj
        self.max_bytes_per_sec = max_bytes_per_sec
        self.last_time = time.time()
        self.bytes_since_last = 0

    def read(self, size: int) -> bytes:
        chunk = self.file.read(size)
        self._throttle(len(chunk))
        return chunk

    def write(self, data: bytes) -> int:
        count = self.file.write(data)
        self._throttle(len(data))
        return count

    def _throttle(self, bytes_transferred: int):
        if self.max_bytes_per_sec <= 0 or bytes_transferred == 0:
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
        return getattr(self.file, attr)


class SFTPDownloader:
    """
    Handles robust SFTP file downloading with resume support and integrity checks.
    """
    def __init__(self, pool):
        """
        Args:
            pool: SSHConnectionPool instance.
        """
        self.pool = pool

    def download_file(
        self,
        remote_path: str,
        local_path: Path,
        file_tracker: Any, # FileTransferTracker
        torrent_hash: str,
        progress_callback: Callable[[int, int], None],
        dry_run: bool = False,
        download_limit: int = 0,
        chunk_size: int = 65536,
        force_integrity_check: bool = False
    ):
        """
        Downloads a single file from the remote path to the local path.
        
        Args:
            progress_callback: Callback(current_total_valid_bytes, network_delta_bytes)
        """
        file_name = os.path.basename(remote_path)
        
        # Check corruption
        if file_tracker.is_corrupted(torrent_hash, remote_path):
             raise RemoteTransferError(f"File {file_name} is marked as corrupted, skipping.")

        local_path.parent.mkdir(parents=True, exist_ok=True)

        with self.pool.get_connection() as (sftp, ssh):
            # 1. Stat Remote
            try:
                remote_stat = sftp.stat(remote_path)
                remote_size = remote_stat.st_size
            except FileNotFoundError:
                logging.error(f"Remote file not found: {remote_path}")
                raise

            if remote_size == 0:
                logging.warning(f"Skipping zero-byte file: {file_name}")
                progress_callback(0, 0)
                return

            # 2. Check Local
            local_size = 0
            if local_path.exists():
                local_size = local_path.stat().st_size

            # 3. Resume Logic
            if local_size == remote_size:
                if not force_integrity_check:
                    logging.info(f"Skipping (exists and size matches): {file_name}")
                    progress_callback(remote_size, 0)
                    return
                else:
                    logging.info(f"Integrity check forced for {file_name}...")
            
            if local_size > remote_size:
                logging.warning(f"Local file larger than remote ({local_size} > {remote_size}). Redownloading.")
                local_size = 0
                file_tracker.record_file_progress(torrent_hash, remote_path, 0) 
            elif local_size > 0:
                logging.info(f"Resuming {file_name} from {local_size / (1024*1024):.2f} MB")
                progress_callback(local_size, 0)

            if dry_run:
                logging.info(f"[DRY RUN] Would download: {remote_path} -> {local_path}")
                progress_callback(remote_size, 0)
                return

            # 4. Transfer
            mode = 'ab' if local_size > 0 else 'wb'
            start_time = time.time()
            
            with sftp.open(remote_path, 'rb') as remote_f_raw:
                remote_f_raw.seek(local_size)
                remote_f_raw.prefetch()
                remote_f = RateLimitedFile(remote_f_raw, download_limit)
                
                with open(local_path, mode) as local_f:
                    while True:
                        chunk = remote_f.read(chunk_size)
                        if not chunk: 
                            break
                        local_f.write(chunk)
                        chunk_len = len(chunk)
                        local_size += chunk_len
                        
                        file_tracker.record_file_progress(torrent_hash, remote_path, local_size)
                        progress_callback(local_size, chunk_len)

            # 5. Verification
            final_local_size = local_path.stat().st_size
            if final_local_size != remote_size:
                raise RemoteTransferError(f"Size mismatch: Expected {remote_size}, got {final_local_size}")
