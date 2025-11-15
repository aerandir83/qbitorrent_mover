from __future__ import annotations
import abc
import dataclasses
import logging
import os
import shlex
import socket
import time
import typing
import tempfile
import shutil
import subprocess
import re
import configparser
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import paramiko

if typing.TYPE_CHECKING:
    from ..core_logic.ssh_manager import SSHConnectionPool
    from ..torrent_mover import Torrent
    from ..ui import BaseUIManager
    from ..core_logic.transfer_manager import FileTransferTracker

# New imports moved for strategies
from ..core_logic.resilience import ResilientTransferQueue
from ..utils import RemoteTransferError, retry
from ..core_logic.ssh_manager import sftp_mkdir_p, _get_ssh_command, is_remote_dir
from ..core_logic.transfer_manager import RateLimitedFile, Timeouts, _parse_human_readable_bytes, _create_safe_command_for_logging

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class TransferFile:
    """Normalized file representation."""
    source_path: str
    dest_path: str
    size: int
    torrent_hash: str


class TransferStrategy(abc.ABC):
    """Abstract base class for different transfer strategies."""

    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        self.config = config
        self.ssh_connection_pools = ssh_connection_pools

    @abc.abstractmethod
    def execute(self, files: List[TransferFile], torrent_info: "Torrent", ui: "BaseUIManager", file_tracker: "FileTransferTracker") -> bool:
        """Executes the transfer strategy."""
        pass

    @abc.abstractmethod
    def prepare_files(self, torrent: "Torrent", dest_path: str) -> List[TransferFile]:
        """Prepare a list of files to be transferred for a given torrent."""
        pass

    @abc.abstractmethod
    def supports_parallel(self) -> bool:
        """Returns True if the strategy supports parallel file transfers."""
        pass

    @abc.abstractmethod
    def supports_delta_correction(self) -> bool:
        """Returns True if the strategy supports delta-based correction (e.g., rsync)."""
        pass


class SFTPStrategy(TransferStrategy):
    """Strategy for handling file transfers using SFTP."""

    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.pool = self.ssh_connection_pools[source_server_section]

    def supports_parallel(self) -> bool:
        return True

    def supports_delta_correction(self) -> bool:
        return False

    def execute(self, files: List[TransferFile], torrent_info: "Torrent", ui: "BaseUIManager", file_tracker: "FileTransferTracker") -> bool:
        sftp_chunk_size = self.config['SETTINGS'].getint('sftp_chunk_size_kb', 64) * 1024
        download_limit_bytes = int(self.config['SETTINGS'].getfloat('sftp_download_limit_mbps', 0) * 1024 * 1024 / 8)
        max_concurrent_downloads = self.config['SETTINGS'].getint('max_concurrent_downloads', 4)
        dry_run = self.config.getboolean('GENERAL', 'dry_run', fallback=False)
        try:
            self._transfer_content_with_queue(
                files, torrent_info.hash, ui, file_tracker,
                max_concurrent_downloads, dry_run,
                download_limit_bytes, sftp_chunk_size
            )
            return True
        except (RemoteTransferError, Exception) as e:
            logger.error(f"SFTP transfer failed for '{torrent_info.name}': {e}", exc_info=True)
            return False

    def _transfer_content_with_queue(
        self,
        all_files: List[TransferFile],
        torrent_hash: str,
        ui: "BaseUIManager",
        file_tracker: "FileTransferTracker",
        max_concurrent_downloads: int,
        dry_run: bool = False,
        download_limit_bytes_per_sec: int = 0,
        sftp_chunk_size: int = 65536
    ) -> None:
        """Transfers torrent content using a resilient queue."""
        queue = ResilientTransferQueue(max_retries=5)
        for file in all_files:
            queue.add(file)

        server_key = f"{self.pool.host}:{self.pool.port}"

        def worker():
            while True:
                result = queue.get_next(server_key)
                if not result:
                    stats = queue.get_stats()
                    if stats["pending"] > 0:
                        time.sleep(1)
                        continue
                    break

                file, attempt_count = result
                try:
                    self._sftp_download_file_resilient(
                        file, queue, ui, file_tracker, attempt_count,
                        server_key, dry_run, download_limit_bytes_per_sec, sftp_chunk_size
                    )
                except Exception:
                    pass

        with ThreadPoolExecutor(max_workers=max_concurrent_downloads) as executor:
            futures = [executor.submit(worker) for _ in range(max_concurrent_downloads)]
            for future in as_completed(futures):
                future.result()

        final_stats = queue.get_stats()
        if final_stats["failed"] > 0:
            raise RemoteTransferError(f"{final_stats['failed']} files failed for torrent {torrent_hash}")

    def _sftp_download_file_resilient(
        self,
        file: TransferFile,
        queue: ResilientTransferQueue,
        ui: "BaseUIManager",
        file_tracker: "FileTransferTracker",
        attempt_count: int,
        server_key: str,
        dry_run: bool = False,
        download_limit_bytes_per_sec: int = 0,
        sftp_chunk_size: int = 65536
    ) -> None:
        """Wrapper for _sftp_download_file_core that integrates with ResilientTransferQueue."""
        try:
            self._sftp_download_file_core(
                file, ui, file_tracker, dry_run,
                download_limit_bytes_per_sec, sftp_chunk_size
            )
            queue.record_success(file, server_key)
        except (socket.timeout, TimeoutError, paramiko.SSHException) as e:
            if not queue.record_failure(file, server_key, e, attempt_count):
                logger.error(f"SFTP download failed permanently for {file.source_path} due to network error: {e}", exc_info=True)
                raise
        except (PermissionError, FileNotFoundError) as e:
            queue.record_failure(file, server_key, e, 999)
            logger.error(f"SFTP download failed permanently for {file.source_path} due to file/permission error: {e}", exc_info=True)
            raise
        except Exception as e:
            if not queue.record_failure(file, server_key, e, attempt_count):
                logger.error(f"SFTP download failed permanently for {file.source_path} due to unexpected error: {e}", exc_info=True)
                raise

    def _sftp_download_file_core(self, file: TransferFile, ui: "BaseUIManager", file_tracker: "FileTransferTracker", dry_run: bool = False, download_limit_bytes_per_sec: int = 0, sftp_chunk_size: int = 65536) -> None:
        remote_file = file.source_path
        local_file = file.dest_path
        torrent_hash = file.torrent_hash
        local_path = Path(local_file)
        file_name = os.path.basename(remote_file)
        try:
            if file_tracker.is_corrupted(torrent_hash, remote_file):
                raise Exception(f"File {file_name} is marked as corrupted, skipping.")

            ui.start_file_transfer(torrent_hash, remote_file, "downloading")
            with self.pool.get_connection() as (sftp, ssh_client):
                remote_stat = sftp.stat(remote_file)
                total_size = remote_stat.st_size
                if total_size == 0:
                    logger.warning(f"Skipping zero-byte file: {file_name}")
                    return
                local_size = local_path.stat().st_size if local_path.exists() else 0

                with ui._lock:
                    if torrent_hash not in ui._file_progress:
                        ui._file_progress[torrent_hash] = {}
                    ui._file_progress[torrent_hash][remote_file] = (local_size, total_size)

                if local_size >= total_size:
                    logger.info(f"Skipping (exists and size matches): {file_name}")
                    ui.update_torrent_progress(torrent_hash, total_size - local_size, transfer_type='download')
                    return

                if dry_run:
                    logger.info(f"[DRY RUN] Would download: {remote_file} -> {local_path}")
                    ui.update_torrent_progress(torrent_hash, total_size - local_size, transfer_type='download')
                    return

                local_path.parent.mkdir(parents=True, exist_ok=True)
                mode = 'ab' if local_size > 0 else 'wb'
                with sftp.open(remote_file, 'rb') as remote_f_raw, open(local_path, mode) as local_f:
                    remote_f_raw.seek(local_size)
                    remote_f_raw.prefetch()
                    remote_f = RateLimitedFile(remote_f_raw, download_limit_bytes_per_sec)
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
                        file_tracker.record_file_progress(torrent_hash, remote_file, local_size)

                if not file_tracker.verify_file_integrity(local_path, total_size):
                    file_tracker.record_corruption(torrent_hash, remote_file)
                    raise Exception(f"File integrity check failed for {file_name}")
            ui.complete_file_transfer(torrent_hash, remote_file)
        except Exception as e:
            file_tracker.record_corruption(torrent_hash, remote_file)
            ui.fail_file_transfer(torrent_hash, remote_file)
            logger.error(f"Download failed for {file_name}: {e}", exc_info=True)
            raise

    def prepare_files(self, torrent: "Torrent", dest_path: str) -> List[TransferFile]:
        from ..core_logic.ssh_manager import _get_all_files_recursive
        source_path = torrent.content_path.rstrip('/\\')
        file_list_tuples: List[Tuple[str, str, int]] = []
        with self.pool.get_connection() as (sftp, ssh):
            try:
                stat_info = sftp.stat(source_path)
                is_dir = stat_info.st_mode & 0o40000
                if is_dir:
                    _get_all_files_recursive(sftp, source_path, dest_path, file_list_tuples)
                else:
                    file_list_tuples.append((source_path, dest_path, stat_info.st_size))
            except FileNotFoundError:
                logger.warning(f"Source path not found, skipping: {source_path}")
        return [TransferFile(src, dest, size, torrent.hash) for src, dest, size in file_list_tuples]


class RsyncStrategy(TransferStrategy):
    """Strategy for handling file transfers using rsync."""

    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.sftp_config = self.config[source_server_section]

    def supports_parallel(self) -> bool:
        return False

    def supports_delta_correction(self) -> bool:
        return True

    def execute(self, files: List[TransferFile], torrent_info: "Torrent", ui: "BaseUIManager", file_tracker: "FileTransferTracker") -> bool:
        dry_run = self.config.getboolean('GENERAL', 'dry_run', fallback=False)
        rsync_options = shlex.split(self.config['SETTINGS'].get("rsync_options", "-avhHSP --partial --inplace"))
        total_size = sum(f.size for f in files)

        if not files:
            logger.warning(f"No files to transfer for torrent {torrent_info.name}")
            return True

        file = files[0]

        try:
            self._transfer_content_rsync(
                remote_path=file.source_path,
                local_path=file.dest_path,
                torrent_hash=torrent_info.hash,
                rsync_options=rsync_options,
                file_tracker=file_tracker,
                total_size=total_size,
                ui=ui,
                dry_run=dry_run
            )
            return True
        except Exception as e:
            logger.error(f"Rsync transfer failed for '{torrent_info.name}': {e}", exc_info=True)
            return False

    def _transfer_content_rsync(self, remote_path: str, local_path: str, torrent_hash: str, rsync_options: List[str], file_tracker: "FileTransferTracker", total_size: int, ui: "BaseUIManager", dry_run: bool = False):
        if file_tracker.is_corrupted(torrent_hash, remote_path):
            raise Exception(f"Transfer for {os.path.basename(remote_path)} is marked as corrupted, skipping.")

        rsync_file_name = os.path.basename(remote_path)
        ui.log(f"Starting rsync transfer for '{rsync_file_name}'")

        host = self.sftp_config['host']
        port = self.sftp_config.getint('port')
        username = self.sftp_config['username']
        password = self.sftp_config['password']

        local_parent_dir = os.path.dirname(local_path)
        Path(local_parent_dir).mkdir(parents=True, exist_ok=True)

        rsync_options_with_checksum = list(rsync_options)
        if '--checksum' not in rsync_options_with_checksum and '-c' not in rsync_options_with_checksum:
            rsync_options_with_checksum.append('--checksum')
        if "--info=progress2" not in rsync_options_with_checksum:
            rsync_options_with_checksum.append("--info=progress2")

        rsync_command_base = ["stdbuf", "-o0", "sshpass", "-p", password, "rsync", *rsync_options_with_checksum, "-e", _get_ssh_command(port)]
        remote_spec = f"{username}@{host}:{shlex.quote(remote_path)}"
        rsync_command = [*rsync_command_base, remote_spec, local_parent_dir]

        if dry_run:
            logger.info(f"[DRY_RUN] Would execute: {' '.join(_create_safe_command_for_logging(rsync_command))}")
            if total_size > 0:
                ui.update_torrent_progress(torrent_hash, total_size, transfer_type='download')
            ui.log(f"[DRY RUN] Completed rsync transfer for {rsync_file_name}")
            return

        for attempt in range(1, 3):
            process = None
            try:
                process = subprocess.Popen(rsync_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

                if process.stdout:
                    for line in process.stdout:
                        # Real-time progress parsing
                        parts = line.split()
                        if len(parts) > 1 and parts[1].replace(',','').isdigit():
                            transferred_bytes = int(parts[1].replace(',',''))
                            ui.update_torrent_progress(torrent_hash, transferred_bytes - ui._file_progress.get(torrent_hash, {}).get(rsync_file_name, (0,0))[0], transfer_type='download')

                process.wait()
                stderr_output = process.stderr.read()

                if process.returncode in [0, 24]:
                    ui.update_torrent_progress(torrent_hash, total_size, transfer_type='download')
                    logger.info(f"Rsync transfer completed for {rsync_file_name}")
                    return
                else:
                    raise RemoteTransferError(f"Rsync failed with exit code {process.returncode}: {stderr_output}")

            except Exception as e:
                logger.error(f"An exception occurred during rsync for '{rsync_file_name}': {e}", exc_info=True)
                if attempt < 2:
                    logger.warning("Retrying...")
                    time.sleep(5)
                else:
                    file_tracker.record_corruption(torrent_hash, remote_path)
                    ui.fail_file_transfer(torrent_hash, rsync_file_name)
                    raise

    def prepare_files(self, torrent: "Torrent", dest_path: str) -> List[TransferFile]:
        from ..core_logic.ssh_manager import batch_get_remote_sizes
        source_path = torrent.content_path.rstrip('/\\')
        source_server_section = self.config['SETTINGS']['source_server_section']
        pool = self.ssh_connection_pools[source_server_section]
        with pool.get_connection() as (sftp, ssh):
            sizes = batch_get_remote_sizes(ssh, [source_path])
            total_size = sizes.get(source_path, 0)
        return [TransferFile(source_path, dest_path, total_size, torrent.hash)]


class SFTPUploadStrategy(TransferStrategy):
    """Strategy for handling SFTP-to-SFTP transfers."""
    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.source_pool = self.ssh_connection_pools[source_server_section]
        self.dest_pool = self.ssh_connection_pools.get('DESTINATION_SERVER')

    def supports_parallel(self) -> bool:
        return True

    def supports_delta_correction(self) -> bool:
        return False

    def execute(self, files: List[TransferFile], torrent_info: "Torrent", ui: "BaseUIManager", file_tracker: "FileTransferTracker") -> bool:
        if not self.dest_pool:
            logger.error("Destination server is not configured for SFTP Upload.")
            return False

        dry_run = self.config.getboolean('GENERAL', 'dry_run', fallback=False)
        sftp_chunk_size = self.config['SETTINGS'].getint('sftp_chunk_size_kb', 64) * 1024
        download_limit_bytes = int(self.config['SETTINGS'].getfloat('sftp_download_limit_mbps', 0) * 1024 * 1024 / 8)
        upload_limit_bytes = int(self.config['SETTINGS'].getfloat('sftp_upload_limit_mbps', 0) * 1024 * 1024 / 8)
        max_concurrent_downloads = self.config['SETTINGS'].getint('max_concurrent_downloads', 4)
        max_concurrent_uploads = self.config['SETTINGS'].getint('max_concurrent_uploads', 4)
        local_cache = self.config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
        total_size = sum(f.size for f in files)

        try:
            self._transfer_content_sftp_upload(
                files, torrent_info.hash, ui, file_tracker, max_concurrent_downloads,
                max_concurrent_uploads, total_size, dry_run, local_cache,
                download_limit_bytes, upload_limit_bytes, sftp_chunk_size
            )
            return True
        except Exception as e:
            logger.error(f"SFTP Upload failed for '{torrent_info.name}': {e}", exc_info=True)
            return False

    def _transfer_content_sftp_upload(self, all_files: List[TransferFile], torrent_hash: str, ui: "BaseUIManager", file_tracker: "FileTransferTracker", max_concurrent_downloads: int, max_concurrent_uploads: int, total_size: int, dry_run: bool, local_cache_sftp_upload: bool, download_limit_bytes_per_sec: int, upload_limit_bytes_per_sec: int, sftp_chunk_size: int):
        GB_BYTES = 1024**3
        if total_size > GB_BYTES and not local_cache_sftp_upload:
            logger.warning("Forcing local caching for this large transfer to ensure memory safety.")
            local_cache_sftp_upload = True

        if local_cache_sftp_upload:
            temp_dir = Path(tempfile.gettempdir()) / f"torrent_mover_cache_{torrent_hash}"
            temp_dir.mkdir(exist_ok=True)
            transfer_successful = False
            try:
                with ThreadPoolExecutor(max_workers=max_concurrent_downloads) as download_executor, \
                     ThreadPoolExecutor(max_workers=max_concurrent_uploads) as upload_executor:

                    download_futures = {
                        download_executor.submit(self._sftp_download_to_cache, file, temp_dir / os.path.basename(file.source_path), ui, file_tracker, download_limit_bytes_per_sec, sftp_chunk_size): file
                        for file in all_files if not file_tracker.get_cache_location(file.torrent_hash, file.source_path)
                    }

                    upload_futures = [
                        upload_executor.submit(self._sftp_upload_from_cache, Path(file_tracker.get_cache_location(file.torrent_hash, file.source_path)), file, ui, file_tracker, upload_limit_bytes_per_sec, sftp_chunk_size)
                        for file in all_files if file_tracker.get_cache_location(file.torrent_hash, file.source_path)
                    ]

                    for future in as_completed(download_futures):
                        file = download_futures[future]
                        future.result()
                        local_cache_path = temp_dir / os.path.basename(file.source_path)
                        file_tracker.record_cache_location(file.torrent_hash, file.source_path, str(local_cache_path), local_cache_path.stat().st_size)
                        upload_futures.append(upload_executor.submit(self._sftp_upload_from_cache, local_cache_path, file, ui, file_tracker, upload_limit_bytes_per_sec, sftp_chunk_size))

                    for future in as_completed(upload_futures):
                        future.result()
                transfer_successful = True
            finally:
                if transfer_successful:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                    file_tracker.clear_torrent_cache(torrent_hash)
        else:
            with ThreadPoolExecutor(max_workers=max_concurrent_uploads) as executor:
                futures = [executor.submit(self._sftp_upload_file, file, ui, file_tracker, dry_run, download_limit_bytes_per_sec, upload_limit_bytes_per_sec, sftp_chunk_size) for file in all_files]
                for future in as_completed(futures):
                    future.result()

    @retry(tries=2, delay=5)
    def _sftp_download_to_cache(self, file: TransferFile, local_cache_path: Path, ui: "BaseUIManager", file_tracker: "FileTransferTracker", download_limit_bytes_per_sec: int, sftp_chunk_size: int):
        self._sftp_download_file_core(file, ui, file_tracker, False, download_limit_bytes_per_sec, sftp_chunk_size, dest_path=local_cache_path)

    @retry(tries=2, delay=5)
    def _sftp_upload_from_cache(self, local_cache_path: Path, file: TransferFile, ui: "BaseUIManager", file_tracker: "FileTransferTracker", upload_limit_bytes_per_sec: int, sftp_chunk_size: int):
        if not local_cache_path.is_file():
            raise FileNotFoundError(f"Local cache file not found: {local_cache_path}")

        ui.start_file_transfer(file.torrent_hash, file.source_path, "uploading")
        total_size = local_cache_path.stat().st_size

        with self.dest_pool.get_connection() as (sftp, _):
            dest_size = 0
            try:
                dest_size = sftp.stat(file.dest_path).st_size
            except FileNotFoundError:
                pass

            if dest_size >= total_size:
                return

            dest_dir = os.path.dirname(file.dest_path)
            sftp_mkdir_p(sftp, dest_dir)

            with open(local_cache_path, 'rb') as source_f_raw:
                source_f_raw.seek(dest_size)
                source_f = RateLimitedFile(source_f_raw, upload_limit_bytes_per_sec)
                with sftp.open(file.dest_path, 'ab' if dest_size > 0 else 'wb') as dest_f:
                    while True:
                        chunk = source_f.read(sftp_chunk_size)
                        if not chunk: break
                        dest_f.write(chunk)
                        dest_size += len(chunk)
                        ui.update_torrent_progress(file.torrent_hash, len(chunk), transfer_type='upload')
        ui.complete_file_transfer(file.torrent_hash, file.source_path)

    def _sftp_upload_file(self, file: TransferFile, ui: "BaseUIManager", file_tracker: "FileTransferTracker", dry_run: bool, download_limit_bytes_per_sec: int, upload_limit_bytes_per_sec: int, sftp_chunk_size: int):
        if dry_run:
            logger.info(f"[DRY RUN] Would upload: {file.source_path} -> {file.dest_path}")
            return

        with self.source_pool.get_connection() as (source_sftp, _), self.dest_pool.get_connection() as (dest_sftp, _):
            dest_dir = os.path.dirname(file.dest_path)
            sftp_mkdir_p(dest_sftp, dest_dir)

            with source_sftp.open(file.source_path, 'rb') as source_f_raw, dest_sftp.open(file.dest_path, 'wb') as dest_f_raw:
                source_f = RateLimitedFile(source_f_raw, download_limit_bytes_per_sec)
                dest_f = RateLimitedFile(dest_f_raw, upload_limit_bytes_per_sec)

                while True:
                    chunk = source_f.read(sftp_chunk_size)
                    if not chunk: break
                    dest_f.write(chunk)
                    ui.update_torrent_progress(file.torrent_hash, len(chunk), transfer_type='upload')

    def prepare_files(self, torrent: "Torrent", dest_path: str) -> List[TransferFile]:
        from ..core_logic.ssh_manager import _get_all_files_recursive
        source_path = torrent.content_path.rstrip('/\\')
        file_list_tuples: List[Tuple[str, str, int]] = []
        with self.source_pool.get_connection() as (sftp, ssh):
            try:
                stat_info = sftp.stat(source_path)
                is_dir = stat_info.st_mode & 0o40000
                if is_dir:
                    _get_all_files_recursive(sftp, source_path, dest_path, file_list_tuples)
                else:
                    file_list_tuples.append((source_path, dest_path, stat_info.st_size))
            except FileNotFoundError:
                logger.warning(f"Source path not found, skipping: {source_path}")
        return [TransferFile(src, dest, size, torrent.hash) for src, dest, size in file_list_tuples]


class RsyncUploadStrategy(TransferStrategy):
    """Strategy for handling rsync-based server-to-server transfers."""
    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS'].get('source_server_section', 'SOURCE_SERVER')
        self.source_config = self.config[source_server_section]
        self.dest_config = self.config['DESTINATION_SERVER']

    def supports_parallel(self) -> bool:
        return False

    def supports_delta_correction(self) -> bool:
        return True

    def execute(self, files: List[TransferFile], torrent_info: "Torrent", ui: "BaseUIManager", file_tracker: "FileTransferTracker") -> bool:
        dry_run = self.config.getboolean('GENERAL', 'dry_run', fallback=False)
        rsync_options = shlex.split(self.config['SETTINGS'].get("rsync_options", "-avhHSP --partial --inplace"))
        total_size = sum(f.size for f in files)

        if not files: return True

        file = files[0]

        source_server_section = self.config['SETTINGS'].get('source_server_section', 'SOURCE_SERVER')
        source_pool = self.ssh_connection_pools.get(source_server_section)
        if not source_pool:
            logger.error(f"SSH pool '{source_server_section}' not found.")
            return False

        try:
            with source_pool.get_connection() as (_, ssh):
                is_folder = is_remote_dir(ssh, torrent_info.content_path)

            self._transfer_content_rsync_upload(
                rsync_options=rsync_options,
                source_content_path=file.source_path,
                dest_content_path=file.dest_path,
                torrent_hash=torrent_info.hash,
                file_tracker=file_tracker,
                total_size=total_size,
                ui=ui,
                dry_run=dry_run,
                is_folder=is_folder
            )
            return True
        except Exception as e:
            logger.error(f"Rsync Upload failed for '{torrent_info.name}': {e}", exc_info=True)
            return False

    def _transfer_content_rsync_upload(self, rsync_options: List[str], source_content_path: str, dest_content_path: str, torrent_hash: str, file_tracker: "FileTransferTracker", total_size: int, ui: "BaseUIManager", dry_run: bool, is_folder: bool) -> bool:
        file_name = os.path.basename(source_content_path)
        temp_dir = Path(tempfile.gettempdir()) / f"torrent_mover_cache_{torrent_hash}"
        temp_dir.mkdir(exist_ok=True)
        local_cache_content_path = str(temp_dir / file_name)

        try:
            rsync_downloader = RsyncStrategy(self.config, self.ssh_connection_pools)
            rsync_downloader._transfer_content_rsync(
                remote_path=source_content_path,
                local_path=local_cache_content_path,
                torrent_hash=torrent_hash,
                rsync_options=rsync_options,
                file_tracker=file_tracker,
                total_size=total_size / 2,
                ui=ui,
                dry_run=dry_run
            )

            self._transfer_content_rsync_upload_from_cache(
                local_path=local_cache_content_path,
                remote_path=dest_content_path,
                torrent_hash=torrent_hash,
                rsync_options=rsync_options,
                total_size=total_size / 2,
                ui=ui,
                dry_run=dry_run
            )
            return True
        finally:
            if not dry_run:
                shutil.rmtree(temp_dir)

    def _transfer_content_rsync_upload_from_cache(self, local_path: str, remote_path: str, torrent_hash: str, rsync_options: List[str], total_size: int, ui: "BaseUIManager", dry_run: bool = False):
        file_name = os.path.basename(local_path)
        ui.log(f"Starting rsync upload from cache for '{file_name}'")

        host = self.dest_config['host']
        port = self.dest_config.getint('port')
        username = self.dest_config['username']
        password = self.dest_config['password']

        remote_parent_dir = os.path.dirname(remote_path)
        remote_spec = f"{username}@{host}:{shlex.quote(remote_parent_dir)}"
        rsync_cmd = ["sshpass", "-p", password, "rsync", *rsync_options, "-e", _get_ssh_command(port), local_path, remote_spec]

        if dry_run:
            logger.info(f"[DRY RUN] Would execute rsync upload: {' '.join(_create_safe_command_for_logging(rsync_cmd))}")
            return

        process = subprocess.run(rsync_cmd, check=True, capture_output=True, text=True)
        logger.info(f"Rsync upload from cache completed for '{file_name}'.")

    def prepare_files(self, torrent: "Torrent", dest_path: str) -> List[TransferFile]:
        from ..core_logic.ssh_manager import batch_get_remote_sizes
        source_path = torrent.content_path.rstrip('/\\')
        source_server_section = self.config['SETTINGS']['source_server_section']
        pool = self.ssh_connection_pools[source_server_section]
        with pool.get_connection() as (sftp, ssh):
            sizes = batch_get_remote_sizes(ssh, [source_path])
            total_size = sizes.get(source_path, 0)
        return [TransferFile(source_path, dest_path, total_size, torrent.hash)]

def get_transfer_strategy(mode: str, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> TransferStrategy:
    """Factory function to get the appropriate transfer strategy."""
    strategies = {
        "sftp": SFTPStrategy,
        "rsync": RsyncStrategy,
        "sftp_upload": SFTPUploadStrategy,
        "rsync_upload": RsyncUploadStrategy,
    }
    strategy_class = strategies.get(mode)
    if not strategy_class:
        raise ValueError(f"Unknown transfer mode: {mode}")
    return strategy_class(config, ssh_connection_pools)
