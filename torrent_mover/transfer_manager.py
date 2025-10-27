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
    """Wrapper for file objects that limits read/write speed."""
    def __init__(self, file_obj: Any, max_bytes_per_sec: float):
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
    def __init__(self, checkpoint_file: Path):
        self.file = checkpoint_file
        self.state = self._load()

    def _load(self) -> Dict[str, List[str]]:
        if self.file.exists():
            try:
                return json.loads(self.file.read_text())
            except json.JSONDecodeError:
                logging.warning(f"Could not decode checkpoint file '{self.file}'. Starting fresh.")
                return {"completed": []}
        return {"completed": []}

    def mark_completed(self, torrent_hash: str) -> None:
        if torrent_hash not in self.state["completed"]:
            self.state["completed"].append(torrent_hash)
            self._save()

    def is_completed(self, torrent_hash: str) -> bool:
        return torrent_hash in self.state["completed"]

    def _save(self) -> None:
        try:
            self.file.write_text(json.dumps(self.state, indent=2))
        except IOError as e:
            logging.error(f"Failed to save checkpoint file '{self.file}': {e}")


class FileTransferTracker:
    """Track partial file transfers for resume capability."""

    def __init__(self, checkpoint_file: Path):
        self.file = checkpoint_file
        self.state = self._load()
        self._lock = threading.Lock()

    def _load(self) -> Dict[str, Any]:
        if self.file.exists():
            try:
                return json.loads(self.file.read_text())
            except json.JSONDecodeError:
                logging.warning(f"Could not decode file transfer tracker file '{self.file}'. Starting fresh.")
                return {"files": {}}
        return {"files": {}}

    def record_file_progress(self, torrent_hash: str, file_path: str, bytes_transferred: int) -> None:
        """Record progress for a specific file."""
        with self._lock:
            key = f"{torrent_hash}:{file_path}"
            self.state["files"][key] = {
                "bytes": bytes_transferred,
                "timestamp": time.time()
            }
            self._save()

    def get_file_progress(self, torrent_hash: str, file_path: str) -> int:
        """Get last known progress for a file."""
        with self._lock:
            key = f"{torrent_hash}:{file_path}"
            return self.state["files"].get(key, {}).get("bytes", 0)

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

def _get_ssh_command(port: int) -> str:
    """Builds the SSH command for rsync, enabling connection multiplexing if available."""
    base_ssh_cmd = f"ssh -p {port} -c aes128-ctr -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=15"
    if SSH_CONTROL_PATH:
        multiplex_opts = f"-o ControlMaster=auto -o ControlPath={shlex.quote(SSH_CONTROL_PATH)} -o ControlPersist=60s"
        return f"{base_ssh_cmd} {multiplex_opts}"
    return base_ssh_cmd

@retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
def _sftp_download_to_cache(source_pool: SSHConnectionPool, source_file_path: str, local_cache_path: Path, torrent_hash: str, ui: UIManager, file_tracker: FileTransferTracker, download_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """
    Downloads a file from SFTP to a local cache path, with resume support and progress reporting.
    """
    try:
        with source_pool.get_connection() as (sftp, ssh):
            remote_stat = sftp.stat(source_file_path)
            total_size = remote_stat.st_size
            ui.start_file_transfer(torrent_hash, source_file_path, total_size)
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
                ui.update_torrent_progress(torrent_hash, total_size - local_size)
                ui.update_file_progress(source_file_path, total_size)
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
                        ui.update_torrent_progress(torrent_hash, increment)
                        # ui.advance_overall_progress(increment)
                        ui.update_file_progress(source_file_path, increment)
                        file_tracker.record_file_progress(torrent_hash, source_file_path, local_size)
    except Exception as e:
        logging.error(f"Failed to download to cache for {source_file_path}: {e}")
        raise
    finally:
        ui.complete_file_transfer(source_file_path)

@retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
def _sftp_upload_from_cache(dest_pool: SSHConnectionPool, local_cache_path: Path, source_file_path: str, dest_file_path: str, torrent_hash: str, ui: UIManager, file_tracker: FileTransferTracker, upload_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """
    Uploads a file from a local cache path to the destination SFTP server.
    """
    file_name = local_cache_path.name
    if not local_cache_path.is_file():
        logging.error(f"Cannot upload '{file_name}' from cache: File does not exist.")
        # ui.update_file_status(torrent_hash, source_file_path, "[red]Cache file missing[/red]")
        ui.complete_file_transfer(source_file_path)
        raise FileNotFoundError(f"Local cache file not found: {local_cache_path}")
    try:
        total_size = local_cache_path.stat().st_size
        ui.start_file_transfer(torrent_hash, source_file_path, total_size)
        # ui.update_file_status(torrent_hash, source_file_path, "Uploading")
        with dest_pool.get_connection() as (sftp, ssh):
            dest_size = 0
            try:
                dest_size = sftp.stat(dest_file_path).st_size
            except FileNotFoundError:
                pass
            if dest_size >= total_size:
                logging.info(f"Skipping upload (exists and size matches): {file_name}")
                ui.update_torrent_progress(torrent_hash, total_size)
                # ui.advance_overall_progress(total_size)
                ui.update_file_progress(source_file_path, total_size)
                return
            if dest_size > 0:
                ui.update_torrent_progress(torrent_hash, dest_size)
                # ui.advance_overall_progress(dest_size)
                ui.update_file_progress(source_file_path, dest_size)
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
                        ui.update_torrent_progress(torrent_hash, increment)
                        # ui.advance_overall_progress(increment)
                        ui.update_file_progress(source_file_path, increment)
            if sftp.stat(dest_file_path).st_size != total_size:
                raise Exception("Final size mismatch during cached upload.")
            # ui.update_file_status(torrent_hash, source_file_path, "[green]Completed[/green]")
    except Exception as e:
        # ui.update_file_status(torrent_hash, source_file_path, "[red]Upload Failed[/red]")
        logging.error(f"Upload from cache failed for {file_name}: {e}")
        raise
    finally:
        ui.complete_file_transfer(source_file_path)

def _sftp_upload_file(source_pool: SSHConnectionPool, dest_pool: SSHConnectionPool, source_file_path: str, dest_file_path: str, torrent_hash: str, ui: UIManager, file_tracker: FileTransferTracker, dry_run: bool = False, download_limit_bytes_per_sec: int = 0, upload_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """
    Streams a single file from a source SFTP server to a destination SFTP server with a progress bar.
    Establishes its own SFTP sessions for thread safety. Supports resuming.
    """
    file_name = os.path.basename(source_file_path)
    try:
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
                    ui.update_torrent_progress(torrent_hash, total_size - dest_size)
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
                ui.update_torrent_progress(torrent_hash, dest_size)
                # ui.advance_overall_progress(dest_size)
            if dry_run:
                logging.info(f"[DRY RUN] Would upload: {source_file_path} -> {dest_file_path}")
                remaining_size = total_size - dest_size
                ui.update_torrent_progress(torrent_hash, remaining_size)
                # ui.advance_overall_progress(remaining_size)
                return
            dest_dir = os.path.dirname(dest_file_path)
            sftp_mkdir_p(dest_sftp, dest_dir)
            try:
                ui.start_file_transfer(torrent_hash, source_file_path, total_size)
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
                            ui.update_torrent_progress(torrent_hash, increment)
                            # ui.advance_overall_progress(increment)
                            ui.update_file_progress(source_file_path, increment)
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
            except PermissionError:
                logging.error(f"Permission denied on destination server for path: {dest_file_path}\n"
                              "Please check that the destination user has write access to that directory.")
                raise
            except FileNotFoundError as e:
                logging.error(f"Source file not found: {source_file_path}")
                logging.error("This can happen if the file was moved or deleted on the source before transfer.")
                raise e
            except (socket.timeout, TimeoutError) as e:
                logging.error(f"Network timeout during upload of file: {file_name}")
                logging.error("The script will retry, but check your network stability if this persists.")
                raise e
            except Exception as e:
                logging.error(f"Upload failed for {file_name}: {e}")
                raise
    finally:
        ui.complete_file_transfer(source_file_path)

@retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
def _sftp_download_file(pool: SSHConnectionPool, remote_file: str, local_file: str, torrent_hash: str, ui: UIManager, file_tracker: FileTransferTracker, dry_run: bool = False, download_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """
    Downloads a single file with a progress bar, with retries. Establishes its own SFTP session
    to ensure thread safety when called from a ThreadPoolExecutor.
    """
    local_path = Path(local_file)
    file_name = os.path.basename(remote_file)
    try:
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
                ui.update_torrent_progress(torrent_hash, total_size - local_size)
                ui.update_file_progress(remote_file, total_size)
                return
            elif local_size > total_size:
                logging.warning(f"Local file '{file_name}' is larger than remote ({local_size} > {total_size}), re-downloading from scratch.")
                local_size = 0
            elif local_size > 0:
                logging.info(f"Resuming download for {file_name} from {local_size / (1024*1024):.2f} MB.")

            if local_size > 0:
                ui.update_torrent_progress(torrent_hash, local_size)
                # ui.advance_overall_progress(local_size)
            if dry_run:
                logging.info(f"[DRY RUN] Would download: {remote_file} -> {local_path}")
                remaining_size = total_size - local_size
                ui.update_torrent_progress(torrent_hash, remaining_size)
                # ui.advance_overall_progress(remaining_size)
                return
            local_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                ui.start_file_transfer(torrent_hash, remote_file, total_size)
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
                            ui.update_torrent_progress(torrent_hash, increment)
                            # ui.advance_overall_progress(increment)
                            ui.update_file_progress(remote_file, increment)
                            file_tracker.record_file_progress(torrent_hash, remote_file, local_size)
                final_local_size = local_path.stat().st_size
                if final_local_size != total_size:
                    raise Exception(f"Final size mismatch for {file_name}. Expected {total_size}, got {final_local_size}")
            except PermissionError:
                logging.error(f"Permission denied while trying to write to local path: {local_path.parent}\n"
                              "Please check that the user running the script has write permissions for this directory.\n"
                              "If you intended to transfer to another remote server, use 'transfer_mode = sftp_upload' in your config.")
                raise
            except FileNotFoundError as e:
                logging.error(f"Source file not found: {remote_file}")
                logging.error("This can happen if the file was moved or deleted on the source before transfer.")
                raise e
            except (socket.timeout, TimeoutError) as e:
                logging.error(f"Network timeout during download of file: {file_name}")
                logging.error("The script will retry, but check your network stability if this persists.")
                raise e
            except Exception as e:
                logging.error(f"Download failed for {file_name}: {e}")
                raise
    finally:
        ui.complete_file_transfer(remote_file)

def transfer_content_rsync(sftp_config: configparser.SectionProxy, remote_path: str, local_path: str, torrent_hash: str, ui: UIManager, dry_run: bool = False) -> None:
    """
    Transfers a remote file or directory to a local path using rsync, with retries.
    """
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
            ui.update_torrent_progress(torrent_hash, total_size)
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
                                ui.update_torrent_progress(torrent_hash, advance)
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
                    ui.update_torrent_progress(torrent_hash, remaining)

                logging.info(f"Rsync transfer completed successfully for '{os.path.basename(remote_path)}'.")
                ui.log(f"[green]Rsync complete: {os.path.basename(remote_path)}[/green]")
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
                raise Exception(f"Rsync transfer failed for {os.path.basename(remote_path)}")
        except FileNotFoundError:
            logging.error("FATAL: 'rsync' or 'sshpass' command not found.")
            raise
        except Exception as e:
            logging.error(f"An exception occurred during rsync for '{os.path.basename(remote_path)}': {e}")
            if process:
                process.kill()
            if attempt < MAX_RETRY_ATTEMPTS:
                logging.warning("Retrying...")
                continue
            else:
                raise e
    raise Exception(f"Rsync transfer for '{os.path.basename(remote_path)}' failed after {MAX_RETRY_ATTEMPTS} attempts.")

def transfer_content(pool: SSHConnectionPool, all_files: List[tuple[str, str]], torrent_hash: str, ui: UIManager, file_tracker: FileTransferTracker, max_concurrent_downloads: int, dry_run: bool = False, download_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
    """
    Transfers a list of remote files to their local destination paths.
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
    """
    Transfers a list of files from a source SFTP to a destination SFTP server.
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
