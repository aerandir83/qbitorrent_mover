from __future__ import annotations

import abc
import configparser
import dataclasses
import logging
import os
import shlex
import argparse
import time
from pathlib import Path
from typing import cast, Dict, List, Optional, TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    import qbittorrentapi
    from .ssh_manager import SSHConnectionPool, batch_get_remote_sizes, is_remote_dir
    from .ui import BaseUIManager
    from .transfer_manager import (
        FileTransferTracker,
        transfer_content_rsync,
        transfer_content_with_queue,
        transfer_content_sftp_upload,
        transfer_content_rsync_upload,
    )
    from .system_manager import change_ownership, delete_destination_content, delete_destination_files
    from .qbittorrent_manager import get_incomplete_files, wait_for_recheck_completion

logger = logging.getLogger(__name__)


def _normalize_path(path: str) -> str:
    """Remove surrounding quotes from paths returned by qBittorrent."""
    return path.strip('\'"')


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
        self.transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

    @abc.abstractmethod
    def pre_transfer_check(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        total_size: int,
        args: argparse.Namespace,
    ) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
        """
        Performs setup tasks before a torrent transfer begins.
        Should return a tuple of:
        - status_code (str)
        - message (str)
        - source_content_path (Optional[str])
        - dest_content_path (Optional[str])
        - destination_save_path (Optional[str])
        """
        pass

    @abc.abstractmethod
    def prepare_files(self, torrent: "qbittorrentapi.TorrentDictionary", dest_path: str) -> List[TransferFile]:
        """Prepare a list of files to be transferred for a given torrent."""
        pass

    @abc.abstractmethod
    def execute_transfer(
        self,
        files: List[TransferFile],
        torrent: "qbittorrentapi.TorrentDictionary",
        ui: "BaseUIManager",
        file_tracker: "FileTransferTracker",
        dry_run: bool,
    ) -> bool:
        """Executes the file transfer for a given list of files."""
        pass

    @abc.abstractmethod
    def post_transfer_actions(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        source_qbit: "qbittorrentapi.Client",
        destination_qbit: Optional["qbittorrentapi.Client"],
        tracker_rules: Dict[str, str],
        dest_content_path: str,
        destination_save_path: str,
        transfer_executed: bool,
        dry_run: bool,
        test_run: bool,
        file_tracker: "FileTransferTracker",
        all_files: List[TransferFile],
        ui: "BaseUIManager",
    ) -> Tuple[bool, str]:
        """Manages tasks after the file transfer is complete."""
        pass


class SFTPStrategy(TransferStrategy):
    """Strategy for handling file transfers using SFTP."""

    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.pool = self.ssh_connection_pools[source_server_section]

    def pre_transfer_check(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        total_size: int,
        args: argparse.Namespace,
    ) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
        source_content_path = _normalize_path(torrent.content_path.rstrip('/\\'))
        dest_base_path = self.config['DESTINATION_PATHS']['destination_path']
        try:
            relative_path = os.path.normpath(os.path.relpath(source_content_path, torrent.save_path))
            content_name = relative_path.split(os.sep)[0]
            original_basename = os.path.basename(source_content_path)
            if content_name != original_basename:
                source_content_path = os.path.join(torrent.save_path, content_name)
        except ValueError:
            content_name = os.path.basename(source_content_path)

        dest_content_path = os.path.join(dest_base_path, content_name)
        remote_dest_base_path = self.config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
        destination_save_path = remote_dest_base_path

        destination_exists = False
        destination_size = -1

        if args.dry_run:
            pass
        else:
            try:
                if os.path.exists(dest_content_path):
                    destination_exists = True
                    if os.path.isdir(dest_content_path):
                        destination_size = sum(f.stat().st_size for f in Path(dest_content_path).glob('**/*') if f.is_file())
                    else:
                        destination_size = os.path.getsize(dest_content_path)
            except Exception as e:
                return "failed", f"Failed to check destination path: {e}", None, None, None

        status_code = "not_exists"
        status_message = "Destination path is clear."

        if destination_exists:
            if total_size == destination_size:
                status_code = "exists_same_size"
                status_message = f"Destination content exists and size matches source ({total_size} bytes). Skipping transfer."
            else:
                status_code = "exists_different_size"
                status_message = (
                    f"Destination content exists but size mismatches "
                    f"(Source: {total_size} vs Dest: {destination_size}). "
                    f"Will delete and re-transfer."
                )
        return status_code, status_message, source_content_path, dest_content_path, destination_save_path

    def supports_parallel(self) -> bool:
        return True

    def prepare_files(self, torrent: "qbittorrentapi.TorrentDictionary", dest_path: str) -> List[TransferFile]:
        from .ssh_manager import _get_all_files_recursive
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
                pass
        return [
            TransferFile(
                source_path=src,
                dest_path=dest,
                size=size,
                torrent_hash=torrent.hash
            ) for src, dest, size in file_list_tuples
        ]

    def execute_transfer(
        self,
        files: List[TransferFile],
        torrent: "qbittorrentapi.TorrentDictionary",
        ui: "BaseUIManager",
        file_tracker: "FileTransferTracker",
        dry_run: bool,
    ) -> bool:
        sftp_chunk_size = self.config['SETTINGS'].getint('sftp_chunk_size_kb', 64) * 1024
        download_limit_bytes = int(self.config['SETTINGS'].getfloat('sftp_download_limit_mbps', 0) * 1024 * 1024 / 8)
        max_concurrent_downloads = self.config['SETTINGS'].getint('max_concurrent_downloads', 4)

        transfer_content_with_queue(
            self.pool, files, torrent.hash, ui, file_tracker,
            max_concurrent_downloads, dry_run,
            download_limit_bytes, sftp_chunk_size
        )
        return True

    def post_transfer_actions(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        source_qbit: "qbittorrentapi.Client",
        destination_qbit: Optional["qbittorrentapi.Client"],
        tracker_rules: Dict[str, str],
        dest_content_path: str,
        destination_save_path: str,
        transfer_executed: bool,
        dry_run: bool,
        test_run: bool,
        file_tracker: "FileTransferTracker",
        all_files: List[TransferFile],
        ui: "BaseUIManager",
    ) -> Tuple[bool, str]:
        name, hash_ = torrent.name, torrent.hash
        if transfer_executed and not dry_run:
            chown_user = self.config['SETTINGS'].get('chown_user', '').strip()
            chown_group = self.config['SETTINGS'].get('chown_group', '').strip()
            if chown_user or chown_group:
                remote_config = None
                path_to_chown = dest_content_path
                if 'DESTINATION_SERVER' in self.config and self.config.has_section('DESTINATION_SERVER'):
                    try:
                        remote_config = self.config['DESTINATION_SERVER']
                        content_name = os.path.basename(dest_content_path)
                        dest_base_path = self.config['DESTINATION_PATHS']['destination_path']
                        remote_dest_base_path = self.config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
                        path_to_chown = os.path.join(remote_dest_base_path, content_name)
                    except Exception as e:
                        remote_config = None
                        path_to_chown = dest_content_path
                change_ownership(path_to_chown, chown_user, chown_group, remote_config, dry_run, self.ssh_connection_pools)

        if not destination_qbit:
            return True, "Transfer complete, no destination client actions performed."

        destination_save_path_str = str(destination_save_path).replace("\\", "/")
        try:
            dest_torrent = destination_qbit.torrents_info(torrent_hashes=hash_)
            if not dest_torrent:
                if not dry_run:
                    try:
                        torrent_file_content = source_qbit.torrents_export(torrent_hash=hash_)
                    except qbittorrentapi.exceptions.NotFound404Error:
                        return False, "Source torrent not found for export"
                    except Exception as e:
                        return False, f"Failed to export .torrent file: {e}"
                    try:
                        destination_qbit.torrents_add(
                            torrent_files=torrent_file_content,
                            save_path=destination_save_path_str,
                            is_paused=True,
                            category=torrent.category,
                            use_auto_torrent_management=True
                        )
                        time.sleep(5)
                    except Exception as e:
                        return False, f"Failed to add torrent to destination: {e}"
        except Exception as e:
            return False, f"Failed to add torrent to destination: {e}"

        if not dry_run:
            try:
                destination_qbit.torrents_recheck(torrent_hashes=hash_)
            except qbittorrentapi.exceptions.NotFound404Error:
                return False, f"Failed to start recheck: Torrent {name} disappeared from destination client."

        recheck_ok = True
        if not dry_run:
            recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=False)

            if not recheck_ok:
                incomplete_files_1 = get_incomplete_files(destination_qbit, torrent.hash)
                if not incomplete_files_1:
                    try:
                        delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                    except Exception as e:
                        pass
                    return False, "Recheck 1 failed, no incomplete files found."

                norm_incomplete_1 = {p.replace('\\', '/') for p in incomplete_files_1}
                delta_files_1 = [
                    f for f in all_files
                    if Path(os.path.relpath(f.dest_path, dest_content_path)).as_posix() in norm_incomplete_1
                ]
                if not delta_files_1:
                    try:
                        delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                    except Exception as e:
                        pass
                    return False, "Recheck 1 failed, file list mismatch."

                delta_1_ok = self.execute_transfer(delta_files_1, torrent, ui, file_tracker, dry_run)

                if not delta_1_ok:
                    return False, "Delta 1 transfer failed."

                try:
                    destination_qbit.torrents_recheck(torrent_hashes=hash_)
                except qbittorrentapi.exceptions.NotFound404Error:
                    return False, "Torrent disappeared during Recheck 2."

                second_recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=False)

                if not second_recheck_ok:
                    incomplete_files_2 = get_incomplete_files(destination_qbit, torrent.hash)
                    if not incomplete_files_2:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Recheck 2 failed, no incomplete files."

                    try:
                        delete_destination_files(dest_content_path, incomplete_files_2, self.transfer_mode, self.ssh_connection_pools)
                    except Exception as e:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e_del:
                            pass
                        return False, f"Partial delete failed: {e}"

                    norm_incomplete_2 = {p.replace('\\', '/') for p in incomplete_files_2}
                    delta_files_2 = [
                        f for f in all_files
                        if Path(os.path.relpath(f.dest_path, dest_content_path)).as_posix() in norm_incomplete_2
                    ]
                    if not delta_files_2:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Recheck 2 failed, file list mismatch."

                    delta_2_ok = self.execute_transfer(delta_files_2, torrent, ui, file_tracker, dry_run)

                    if not delta_2_ok:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Delta 2 transfer failed."

                    try:
                        destination_qbit.torrents_recheck(torrent_hashes=hash_)
                    except qbittorrentapi.exceptions.NotFound404Error:
                        return False, "Torrent disappeared during Recheck 3."

                    third_recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=False)

                    if not third_recheck_ok:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Recheck 3 (final) failed."
                    else:
                        recheck_ok = True

        if recheck_ok:
            try:
                if torrent.category:
                    if not dry_run:
                        destination_qbit.torrents_set_category(torrent_hashes=torrent.hash, category=torrent.category)
                        time.sleep(1)

                if tracker_rules and not dry_run:
                    from .tracker_manager import set_category_based_on_tracker
                    set_category_based_on_tracker(destination_qbit, torrent.hash, tracker_rules, dry_run)
                    time.sleep(1)

                add_paused = self.config.getboolean('DESTINATION_CLIENT', 'add_torrents_paused', fallback=True)
                start_after_recheck = self.config.getboolean('DESTINATION_CLIENT', 'start_torrents_after_recheck', fallback=True)

                if add_paused and start_after_recheck:
                    if not dry_run:
                        destination_qbit.torrents_resume(torrent_hashes=torrent.hash)
                        time.sleep(2)

                if not dry_run and not test_run:
                    if self.config.getboolean('SOURCE_CLIENT', 'delete_after_transfer', fallback=True):
                        try:
                            source_qbit.torrents_delete(torrent_hashes=torrent.hash, delete_files=True)
                            time.sleep(1)
                        except qbittorrentapi.exceptions.NotFound404Error:
                            pass

                return True, "Post-transfer actions completed successfully."
            except Exception as e:
                return False, f"Post-transfer actions failed: {e}"

        return False, "Post-transfer actions failed due to unhandled recheck status."

class RsyncStrategy(TransferStrategy):
    """Strategy for handling file transfers using rsync."""

    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.pool = self.ssh_connection_pools[source_server_section]

    def pre_transfer_check(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        total_size: int,
        args: argparse.Namespace,
    ) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
        source_content_path = _normalize_path(torrent.content_path.rstrip('/\\'))
        dest_base_path = self.config['DESTINATION_PATHS']['destination_path']
        try:
            relative_path = os.path.normpath(os.path.relpath(source_content_path, torrent.save_path))
            content_name = relative_path.split(os.sep)[0]
            original_basename = os.path.basename(source_content_path)
            if content_name != original_basename:
                source_content_path = os.path.join(torrent.save_path, content_name)
        except ValueError:
            content_name = os.path.basename(source_content_path)

        dest_content_path = os.path.join(dest_base_path, content_name)
        remote_dest_base_path = self.config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
        destination_save_path = remote_dest_base_path

        destination_exists = False
        destination_size = -1

        if args.dry_run:
            pass
        else:
            try:
                if os.path.exists(dest_content_path):
                    destination_exists = True
                    if os.path.isdir(dest_content_path):
                        destination_size = sum(f.stat().st_size for f in Path(dest_content_path).glob('**/*') if f.is_file())
                    else:
                        destination_size = os.path.getsize(dest_content_path)
            except Exception as e:
                return "failed", f"Failed to check destination path: {e}", None, None, None

        status_code = "not_exists"
        status_message = "Destination path is clear."

        if destination_exists:
            if total_size == destination_size:
                status_code = "exists_same_size"
                status_message = f"Destination content exists and size matches source ({total_size} bytes). Skipping transfer."
            else:
                status_code = "exists_different_size"
                status_message = (
                    f"Destination content exists but size mismatches "
                    f"(Source: {total_size} vs Dest: {destination_size}). "
                    f"Rsync mode: Will resume transfer."
                )
        return status_code, status_message, source_content_path, dest_content_path, destination_save_path

    def supports_parallel(self) -> bool:
        return False

    def prepare_files(self, torrent: "qbittorrentapi.TorrentDictionary", dest_path: str) -> List[TransferFile]:
        from .ssh_manager import batch_get_remote_sizes
        source_path = torrent.content_path.rstrip('/\\')
        with self.pool.get_connection() as (sftp, ssh):
            sizes = batch_get_remote_sizes(ssh, [source_path])
            total_size = sizes.get(source_path, 0)

        return [
            TransferFile(
                source_path=source_path,
                dest_path=dest_path,
                size=total_size,
                torrent_hash=torrent.hash
            )
        ]

    def execute_transfer(
        self,
        files: List[TransferFile],
        torrent: "qbittorrentapi.TorrentDictionary",
        ui: "BaseUIManager",
        file_tracker: "FileTransferTracker",
        dry_run: bool,
    ) -> bool:
        rsync_options = shlex.split(self.config['SETTINGS'].get("rsync_options", "-avhHSP --partial --inplace"))
        sftp_config = self.config[self.config['SETTINGS']['source_server_section']]
        if not files:
            raise ValueError("No files provided for rsync transfer.")
        source_content_path = files[0].source_path
        dest_content_path = files[0].dest_path
        total_size_calc = sum(f.size for f in files)

        transfer_content_rsync(
            sftp_config=sftp_config,
            remote_path=source_content_path,
            local_path=dest_content_path,
            torrent_hash=torrent.hash,
            log_transfer=lambda _hash, msg: ui.log(msg),
            _update_transfer_progress=ui.update_torrent_progress,
            _update_transfer_speed=ui.update_transfer_speed,
            rsync_options=rsync_options,
            file_tracker=file_tracker,
            total_size=total_size_calc,
            dry_run=dry_run,
            transfer_type='download'
        )
        return True

    def post_transfer_actions(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        source_qbit: "qbittorrentapi.Client",
        destination_qbit: Optional["qbittorrentapi.Client"],
        tracker_rules: Dict[str, str],
        dest_content_path: str,
        destination_save_path: str,
        transfer_executed: bool,
        dry_run: bool,
        test_run: bool,
        file_tracker: "FileTransferTracker",
        all_files: List[TransferFile],
        ui: "BaseUIManager",
    ) -> Tuple[bool, str]:
        name, hash_ = torrent.name, torrent.hash
        if transfer_executed and not dry_run:
            chown_user = self.config['SETTINGS'].get('chown_user', '').strip()
            chown_group = self.config['SETTINGS'].get('chown_group', '').strip()
            if chown_user or chown_group:
                remote_config = None
                path_to_chown = dest_content_path
                if 'DESTINATION_SERVER' in self.config and self.config.has_section('DESTINATION_SERVER'):
                    try:
                        remote_config = self.config['DESTINATION_SERVER']
                        content_name = os.path.basename(dest_content_path)
                        dest_base_path = self.config['DESTINATION_PATHS']['destination_path']
                        remote_dest_base_path = self.config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
                        path_to_chown = os.path.join(remote_dest_base_path, content_name)
                    except Exception as e:
                        remote_config = None
                        path_to_chown = dest_content_path
                change_ownership(path_to_chown, chown_user, chown_group, remote_config, dry_run, self.ssh_connection_pools)

        if not destination_qbit:
            return True, "Transfer complete, no destination client actions performed."

        destination_save_path_str = str(destination_save_path).replace("\\", "/")
        try:
            dest_torrent = destination_qbit.torrents_info(torrent_hashes=hash_)
            if not dest_torrent:
                if not dry_run:
                    try:
                        torrent_file_content = source_qbit.torrents_export(torrent_hash=hash_)
                    except qbittorrentapi.exceptions.NotFound404Error:
                        return False, "Source torrent not found for export"
                    except Exception as e:
                        return False, f"Failed to export .torrent file: {e}"
                    try:
                        destination_qbit.torrents_add(
                            torrent_files=torrent_file_content,
                            save_path=destination_save_path_str,
                            is_paused=True,
                            category=torrent.category,
                            use_auto_torrent_management=True
                        )
                        time.sleep(5)
                    except Exception as e:
                        return False, f"Failed to add torrent to destination: {e}"
        except Exception as e:
            return False, f"Failed to add torrent to destination: {e}"

        if not dry_run:
            try:
                destination_qbit.torrents_recheck(torrent_hashes=hash_)
            except qbittorrentapi.exceptions.NotFound404Error:
                return False, f"Failed to start recheck: Torrent {name} disappeared from destination client."

        recheck_ok = True
        if not dry_run:
            rsync_near_complete = self.config.getboolean('SETTINGS', 'allow_near_complete_rsync', fallback=True)
            recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=rsync_near_complete)

            if not recheck_ok:
                delta_files_1 = all_files
                delta_1_ok = self.execute_transfer(delta_files_1, torrent, ui, file_tracker, dry_run)

                if not delta_1_ok:
                    return False, "Delta 1 transfer failed."

                try:
                    destination_qbit.torrents_recheck(torrent_hashes=hash_)
                except qbittorrentapi.exceptions.NotFound404Error:
                    return False, "Torrent disappeared during Recheck 2."

                second_recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=rsync_near_complete)
                if not second_recheck_ok:
                    incomplete_files_2 = get_incomplete_files(destination_qbit, torrent.hash)
                    if not incomplete_files_2:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Recheck 2 failed, no incomplete files."

                    try:
                        delete_destination_files(dest_content_path, incomplete_files_2, self.transfer_mode, self.ssh_connection_pools)
                    except Exception as e:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e_del:
                            pass
                        return False, f"Partial delete failed: {e}"

                    delta_files_2 = all_files
                    delta_2_ok = self.execute_transfer(delta_files_2, torrent, ui, file_tracker, dry_run)

                    if not delta_2_ok:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Delta 2 transfer failed."

                    try:
                        destination_qbit.torrents_recheck(torrent_hashes=hash_)
                    except qbittorrentapi.exceptions.NotFound404Error:
                        return False, "Torrent disappeared during Recheck 3."

                    third_recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=rsync_near_complete)
                    if not third_recheck_ok:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Recheck 3 (final) failed."
                    else:
                        recheck_ok = True

        if recheck_ok:
            try:
                if torrent.category:
                    if not dry_run:
                        destination_qbit.torrents_set_category(torrent_hashes=torrent.hash, category=torrent.category)
                        time.sleep(1)

                if tracker_rules and not dry_run:
                    from .tracker_manager import set_category_based_on_tracker
                    set_category_based_on_tracker(destination_qbit, torrent.hash, tracker_rules, dry_run)
                    time.sleep(1)

                add_paused = self.config.getboolean('DESTINATION_CLIENT', 'add_torrents_paused', fallback=True)
                start_after_recheck = self.config.getboolean('DESTINATION_CLIENT', 'start_torrents_after_recheck', fallback=True)

                if add_paused and start_after_recheck:
                    if not dry_run:
                        destination_qbit.torrents_resume(torrent_hashes=torrent.hash)
                        time.sleep(2)

                if not dry_run and not test_run:
                    if self.config.getboolean('SOURCE_CLIENT', 'delete_after_transfer', fallback=True):
                        try:
                            source_qbit.torrents_delete(torrent_hashes=torrent.hash, delete_files=True)
                            time.sleep(1)
                        except qbittorrentapi.exceptions.NotFound404Error:
                            pass

                return True, "Post-transfer actions completed successfully."
            except Exception as e:
                return False, f"Post-transfer actions failed: {e}"

        return False, "Post-transfer actions failed due to unhandled recheck status."


class SFTPUploadStrategy(TransferStrategy):
    """Strategy for handling SFTP-to-SFTP transfers."""
    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.source_pool = self.ssh_connection_pools[source_server_section]
        dest_server_section = 'DESTINATION_SERVER'
        self.dest_pool = self.ssh_connection_pools[dest_server_section]

    def pre_transfer_check(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        total_size: int,
        args: argparse.Namespace,
    ) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
        source_content_path = _normalize_path(torrent.content_path.rstrip('/\\'))
        dest_base_path = self.config['DESTINATION_PATHS']['destination_path']
        try:
            relative_path = os.path.normpath(os.path.relpath(source_content_path, torrent.save_path))
            content_name = relative_path.split(os.sep)[0]
            original_basename = os.path.basename(source_content_path)
            if content_name != original_basename:
                source_content_path = os.path.join(torrent.save_path, content_name)
        except ValueError:
            content_name = os.path.basename(source_content_path)

        dest_content_path = os.path.join(dest_base_path, content_name)
        remote_dest_base_path = self.config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
        destination_save_path = remote_dest_base_path

        destination_exists = False
        destination_size = -1

        if args.dry_run:
            pass
        else:
            try:
                with self.dest_pool.get_connection() as (sftp, ssh):
                    size_map = batch_get_remote_sizes(ssh, [dest_content_path])
                    if dest_content_path in size_map:
                        destination_size = size_map[dest_content_path]
                        destination_exists = True
            except Exception as e:
                return "failed", f"Failed to check destination path: {e}", None, None, None

        status_code = "not_exists"
        status_message = "Destination path is clear."

        if destination_exists:
            if total_size == destination_size:
                status_code = "exists_same_size"
                status_message = f"Destination content exists and size matches source ({total_size} bytes). Skipping transfer."
            else:
                status_code = "exists_different_size"
                status_message = (
                    f"Destination content exists but size mismatches "
                    f"(Source: {total_size} vs Dest: {destination_size}). "
                    f"Will delete and re-transfer."
                )
        return status_code, status_message, source_content_path, dest_content_path, destination_save_path

    def supports_parallel(self) -> bool:
        return True

    def prepare_files(self, torrent: "qbittorrentapi.TorrentDictionary", dest_path: str) -> List[TransferFile]:
        from .ssh_manager import _get_all_files_recursive
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
                pass
        return [
            TransferFile(
                source_path=src,
                dest_path=dest,
                size=size,
                torrent_hash=torrent.hash
            ) for src, dest, size in file_list_tuples
        ]

    def execute_transfer(
        self,
        files: List[TransferFile],
        torrent: "qbittorrentapi.TorrentDictionary",
        ui: "BaseUIManager",
        file_tracker: "FileTransferTracker",
        dry_run: bool,
    ) -> bool:
        sftp_chunk_size = self.config['SETTINGS'].getint('sftp_chunk_size_kb', 64) * 1024
        max_concurrent_downloads = self.config['SETTINGS'].getint('max_concurrent_downloads', 4)
        max_concurrent_uploads = self.config['SETTINGS'].getint('max_concurrent_uploads', 4)
        sftp_download_limit_mbps = self.config['SETTINGS'].getfloat('sftp_download_limit_mbps', 0)
        sftp_upload_limit_mbps = self.config['SETTINGS'].getfloat('sftp_upload_limit_mbps', 0)
        download_limit_bytes = int(sftp_download_limit_mbps * 1024 * 1024 / 8)
        upload_limit_bytes = int(sftp_upload_limit_mbps * 1024 * 1024 / 8)
        local_cache = self.config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
        all_files_tuples = [(f.source_path, f.dest_path) for f in files]
        total_size_calc = sum(f.size for f in files)
        transfer_content_sftp_upload(
            self.source_pool, self.dest_pool, all_files_tuples, torrent.hash, ui,
            file_tracker, max_concurrent_downloads, max_concurrent_uploads, total_size_calc, dry_run,
            local_cache, download_limit_bytes, upload_limit_bytes, sftp_chunk_size
        )
        return True

    def post_transfer_actions(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        source_qbit: "qbittorrentapi.Client",
        destination_qbit: Optional["qbittorrentapi.Client"],
        tracker_rules: Dict[str, str],
        dest_content_path: str,
        destination_save_path: str,
        transfer_executed: bool,
        dry_run: bool,
        test_run: bool,
        file_tracker: "FileTransferTracker",
        all_files: List[TransferFile],
        ui: "BaseUIManager",
    ) -> Tuple[bool, str]:
        name, hash_ = torrent.name, torrent.hash
        if transfer_executed and not dry_run:
            chown_user = self.config['SETTINGS'].get('chown_user', '').strip()
            chown_group = self.config['SETTINGS'].get('chown_group', '').strip()
            if chown_user or chown_group:
                remote_config = self.config['DESTINATION_SERVER']
                content_name = os.path.basename(dest_content_path)
                remote_dest_base_path = self.config['DESTINATION_PATHS'].get('remote_destination_path') or self.config['DESTINATION_PATHS']['destination_path']
                path_to_chown = os.path.join(remote_dest_base_path, content_name)
                change_ownership(path_to_chown, chown_user, chown_group, remote_config, dry_run, self.ssh_connection_pools)

        if not destination_qbit:
            return True, "Transfer complete, no destination client actions performed."

        destination_save_path_str = str(destination_save_path).replace("\\", "/")
        try:
            dest_torrent = destination_qbit.torrents_info(torrent_hashes=hash_)
            if not dest_torrent:
                if not dry_run:
                    try:
                        torrent_file_content = source_qbit.torrents_export(torrent_hash=hash_)
                    except qbittorrentapi.exceptions.NotFound404Error:
                        return False, "Source torrent not found for export"
                    except Exception as e:
                        return False, f"Failed to export .torrent file: {e}"
                    try:
                        destination_qbit.torrents_add(
                            torrent_files=torrent_file_content,
                            save_path=destination_save_path_str,
                            is_paused=True,
                            category=torrent.category,
                            use_auto_torrent_management=True
                        )
                        time.sleep(5)
                    except Exception as e:
                        return False, f"Failed to add torrent to destination: {e}"
        except Exception as e:
            return False, f"Failed to add torrent to destination: {e}"

        if not dry_run:
            try:
                destination_qbit.torrents_recheck(torrent_hashes=hash_)
            except qbittorrentapi.exceptions.NotFound404Error:
                return False, f"Failed to start recheck: Torrent {name} disappeared from destination client."

        recheck_ok = True
        if not dry_run:
            recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=False)

            if not recheck_ok:
                incomplete_files_1 = get_incomplete_files(destination_qbit, torrent.hash)
                if not incomplete_files_1:
                    try:
                        delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                    except Exception as e:
                        pass
                    return False, "Recheck 1 failed, no incomplete files found."

                norm_incomplete_1 = {p.replace('\\', '/') for p in incomplete_files_1}
                delta_files_1 = [
                    f for f in all_files
                    if Path(os.path.relpath(f.dest_path, dest_content_path)).as_posix() in norm_incomplete_1
                ]
                if not delta_files_1:
                    try:
                        delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                    except Exception as e:
                        pass
                    return False, "Recheck 1 failed, file list mismatch."

                delta_1_ok = self.execute_transfer(delta_files_1, torrent, ui, file_tracker, dry_run)

                if not delta_1_ok:
                    return False, "Delta 1 transfer failed."

                try:
                    destination_qbit.torrents_recheck(torrent_hashes=hash_)
                except qbittorrentapi.exceptions.NotFound404Error:
                    return False, "Torrent disappeared during Recheck 2."

                second_recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=False)

                if not second_recheck_ok:
                    incomplete_files_2 = get_incomplete_files(destination_qbit, torrent.hash)
                    if not incomplete_files_2:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Recheck 2 failed, no incomplete files."

                    try:
                        delete_destination_files(dest_content_path, incomplete_files_2, self.transfer_mode, self.ssh_connection_pools)
                    except Exception as e:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e_del:
                            pass
                        return False, f"Partial delete failed: {e}"

                    norm_incomplete_2 = {p.replace('\\', '/') for p in incomplete_files_2}
                    delta_files_2 = [
                        f for f in all_files
                        if Path(os.path.relpath(f.dest_path, dest_content_path)).as_posix() in norm_incomplete_2
                    ]
                    if not delta_files_2:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Recheck 2 failed, file list mismatch."

                    delta_2_ok = self.execute_transfer(delta_files_2, torrent, ui, file_tracker, dry_run)

                    if not delta_2_ok:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Delta 2 transfer failed."

                    try:
                        destination_qbit.torrents_recheck(torrent_hashes=hash_)
                    except qbittorrentapi.exceptions.NotFound404Error:
                        return False, "Torrent disappeared during Recheck 3."

                    third_recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=False)

                    if not third_recheck_ok:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Recheck 3 (final) failed."
                    else:
                        recheck_ok = True

        if recheck_ok:
            try:
                if torrent.category:
                    if not dry_run:
                        destination_qbit.torrents_set_category(torrent_hashes=torrent.hash, category=torrent.category)
                        time.sleep(1)

                if tracker_rules and not dry_run:
                    from .tracker_manager import set_category_based_on_tracker
                    set_category_based_on_tracker(destination_qbit, torrent.hash, tracker_rules, dry_run)
                    time.sleep(1)

                add_paused = self.config.getboolean('DESTINATION_CLIENT', 'add_torrents_paused', fallback=True)
                start_after_recheck = self.config.getboolean('DESTINATION_CLIENT', 'start_torrents_after_recheck', fallback=True)

                if add_paused and start_after_recheck:
                    if not dry_run:
                        destination_qbit.torrents_resume(torrent_hashes=torrent.hash)
                        time.sleep(2)

                if not dry_run and not test_run:
                    if self.config.getboolean('SOURCE_CLIENT', 'delete_after_transfer', fallback=True):
                        try:
                            source_qbit.torrents_delete(torrent_hashes=torrent.hash, delete_files=True)
                            time.sleep(1)
                        except qbittorrentapi.exceptions.NotFound404Error:
                            pass

                return True, "Post-transfer actions completed successfully."
            except Exception as e:
                return False, f"Post-transfer actions failed: {e}"

        return False, "Post-transfer actions failed due to unhandled recheck status."

class RsyncUploadStrategy(TransferStrategy):
    """Strategy for handling rsync-based server-to-server transfers."""
    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.source_pool = self.ssh_connection_pools[source_server_section]
        dest_server_section = 'DESTINATION_SERVER'
        self.dest_pool = self.ssh_connection_pools[dest_server_section]

    def pre_transfer_check(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        total_size: int,
        args: argparse.Namespace,
    ) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
        source_content_path = _normalize_path(torrent.content_path.rstrip('/\\'))
        dest_base_path = self.config['DESTINATION_PATHS']['destination_path']
        try:
            relative_path = os.path.normpath(os.path.relpath(source_content_path, torrent.save_path))
            content_name = relative_path.split(os.sep)[0]
            original_basename = os.path.basename(source_content_path)
            if content_name != original_basename:
                source_content_path = os.path.join(torrent.save_path, content_name)
        except ValueError:
            content_name = os.path.basename(source_content_path)

        dest_content_path = os.path.join(dest_base_path, content_name)
        remote_dest_base_path = self.config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
        destination_save_path = remote_dest_base_path

        destination_exists = False
        destination_size = -1

        if args.dry_run:
            pass
        else:
            try:
                with self.dest_pool.get_connection() as (sftp, ssh):
                    size_map = batch_get_remote_sizes(ssh, [dest_content_path])
                    if dest_content_path in size_map:
                        destination_size = size_map[dest_content_path]
                        destination_exists = True
            except Exception as e:
                return "failed", f"Failed to check destination path: {e}", None, None, None

        status_code = "not_exists"
        status_message = "Destination path is clear."

        if destination_exists:
            if total_size == destination_size:
                status_code = "exists_same_size"
                status_message = f"Destination content exists and size matches source ({total_size} bytes). Skipping transfer."
            else:
                status_code = "exists_different_size"
                status_message = (
                    f"Destination content exists but size mismatches "
                    f"(Source: {total_size} vs Dest: {destination_size}). "
                    f"Rsync mode: Will resume transfer."
                )
        return status_code, status_message, source_content_path, dest_content_path, destination_save_path

    def supports_parallel(self) -> bool:
        return False

    def prepare_files(self, torrent: "qbittorrentapi.TorrentDictionary", dest_path: str) -> List[TransferFile]:
        from .ssh_manager import batch_get_remote_sizes
        source_path = torrent.content_path.rstrip('/\\')
        with self.source_pool.get_connection() as (sftp, ssh):
            sizes = batch_get_remote_sizes(ssh, [source_path])
            total_size = sizes.get(source_path, 0)
        return [
            TransferFile(
                source_path=source_path,
                dest_path=dest_path,
                size=total_size,
                torrent_hash=torrent.hash
            )
        ]

    def execute_transfer(
        self,
        files: List[TransferFile],
        torrent: "qbittorrentapi.TorrentDictionary",
        ui: "BaseUIManager",
        file_tracker: "FileTransferTracker",
        dry_run: bool,
    ) -> bool:
        source_server_section = self.config['SETTINGS'].get('source_server_section', 'SOURCE_SERVER')
        source_config = self.config[source_server_section]
        dest_server_section = 'DESTINATION_SERVER'
        dest_config = self.config[dest_server_section]
        rsync_options = shlex.split(self.config['SETTINGS'].get("rsync_options", "-avhHSP --partial --inplace"))

        is_folder = False
        with self.source_pool.get_connection() as (_, ssh):
            is_folder = is_remote_dir(ssh, torrent.content_path)

        if not files:
            raise ValueError("No files provided for rsync upload.")

        source_content_path = files[0].source_path
        dest_content_path = files[0].dest_path
        total_size_calc = sum(f.size for f in files)

        transfer_content_rsync_upload(
            source_config=source_config,
            dest_config=dest_config,
            rsync_options=rsync_options,
            source_content_path=source_content_path,
            dest_content_path=dest_content_path,
            torrent_hash=torrent.hash,
            file_tracker=file_tracker,
            total_size=total_size_calc,
            log_transfer=lambda _hash, msg: ui.log(msg),
            _update_transfer_progress=ui.update_torrent_progress,
            _update_transfer_speed=ui.update_transfer_speed,
            dry_run=dry_run,
            is_folder=is_folder,
            transfer_type='upload'
        )
        return True

    def post_transfer_actions(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        source_qbit: "qbittorrentapi.Client",
        destination_qbit: Optional["qbittorrentapi.Client"],
        tracker_rules: Dict[str, str],
        dest_content_path: str,
        destination_save_path: str,
        transfer_executed: bool,
        dry_run: bool,
        test_run: bool,
        file_tracker: "FileTransferTracker",
        all_files: List[TransferFile],
        ui: "BaseUIManager",
    ) -> Tuple[bool, str]:
        name, hash_ = torrent.name, torrent.hash
        if transfer_executed and not dry_run:
            chown_user = self.config['SETTINGS'].get('chown_user', '').strip()
            chown_group = self.config['SETTINGS'].get('chown_group', '').strip()
            if chown_user or chown_group:
                remote_config = self.config['DESTINATION_SERVER']
                content_name = os.path.basename(dest_content_path)
                remote_dest_base_path = self.config['DESTINATION_PATHS'].get('remote_destination_path') or self.config['DESTINATION_PATHS']['destination_path']
                path_to_chown = os.path.join(remote_dest_base_path, content_name)
                change_ownership(path_to_chown, chown_user, chown_group, remote_config, dry_run, self.ssh_connection_pools)

        if not destination_qbit:
            return True, "Transfer complete, no destination client actions performed."

        destination_save_path_str = str(destination_save_path).replace("\\", "/")
        try:
            dest_torrent = destination_qbit.torrents_info(torrent_hashes=hash_)
            if not dest_torrent:
                if not dry_run:
                    try:
                        torrent_file_content = source_qbit.torrents_export(torrent_hash=hash_)
                    except qbittorrentapi.exceptions.NotFound404Error:
                        return False, "Source torrent not found for export"
                    except Exception as e:
                        return False, f"Failed to export .torrent file: {e}"
                    try:
                        destination_qbit.torrents_add(
                            torrent_files=torrent_file_content,
                            save_path=destination_save_path_str,
                            is_paused=True,
                            category=torrent.category,
                            use_auto_torrent_management=True
                        )
                        time.sleep(5)
                    except Exception as e:
                        return False, f"Failed to add torrent to destination: {e}"
        except Exception as e:
            return False, f"Failed to add torrent to destination: {e}"

        if not dry_run:
            try:
                destination_qbit.torrents_recheck(torrent_hashes=hash_)
            except qbittorrentapi.exceptions.NotFound404Error:
                return False, f"Failed to start recheck: Torrent {name} disappeared from destination client."

        recheck_ok = True
        if not dry_run:
            rsync_near_complete = self.config.getboolean('SETTINGS', 'allow_near_complete_rsync', fallback=True)
            recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=rsync_near_complete)

            if not recheck_ok:
                delta_files_1 = all_files
                delta_1_ok = self.execute_transfer(delta_files_1, torrent, ui, file_tracker, dry_run)

                if not delta_1_ok:
                    return False, "Delta 1 transfer failed."

                try:
                    destination_qbit.torrents_recheck(torrent_hashes=hash_)
                except qbittorrentapi.exceptions.NotFound404Error:
                    return False, "Torrent disappeared during Recheck 2."

                second_recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=rsync_near_complete)
                if not second_recheck_ok:
                    incomplete_files_2 = get_incomplete_files(destination_qbit, torrent.hash)
                    if not incomplete_files_2:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Recheck 2 failed, no incomplete files."

                    try:
                        delete_destination_files(dest_content_path, incomplete_files_2, self.transfer_mode, self.ssh_connection_pools)
                    except Exception as e:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e_del:
                            pass
                        return False, f"Partial delete failed: {e}"

                    delta_files_2 = all_files
                    delta_2_ok = self.execute_transfer(delta_files_2, torrent, ui, file_tracker, dry_run)

                    if not delta_2_ok:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Delta 2 transfer failed."

                    try:
                        destination_qbit.torrents_recheck(torrent_hashes=hash_)
                    except qbittorrentapi.exceptions.NotFound404Error:
                        return False, "Torrent disappeared during Recheck 3."

                    third_recheck_ok = wait_for_recheck_completion(destination_qbit, torrent.hash, ui, allow_near_complete=rsync_near_complete)
                    if not third_recheck_ok:
                        try:
                            delete_destination_content(dest_content_path, self.transfer_mode, self.ssh_connection_pools)
                        except Exception as e:
                            pass
                        return False, "Recheck 3 (final) failed."
                    else:
                        recheck_ok = True

        if recheck_ok:
            try:
                if torrent.category:
                    if not dry_run:
                        destination_qbit.torrents_set_category(torrent_hashes=torrent.hash, category=torrent.category)
                        time.sleep(1)

                if tracker_rules and not dry_run:
                    from .tracker_manager import set_category_based_on_tracker
                    set_category_based_on_tracker(destination_qbit, torrent.hash, tracker_rules, dry_run)
                    time.sleep(1)

                add_paused = self.config.getboolean('DESTINATION_CLIENT', 'add_torrents_paused', fallback=True)
                start_after_recheck = self.config.getboolean('DESTINATION_CLIENT', 'start_torrents_after_recheck', fallback=True)

                if add_paused and start_after_recheck:
                    if not dry_run:
                        destination_qbit.torrents_resume(torrent_hashes=torrent.hash)
                        time.sleep(2)

                if not dry_run and not test_run:
                    if self.config.getboolean('SOURCE_CLIENT', 'delete_after_transfer', fallback=True):
                        try:
                            source_qbit.torrents_delete(torrent_hashes=torrent.hash, delete_files=True)
                            time.sleep(1)
                        except qbittorrentapi.exceptions.NotFound404Error:
                            pass

                return True, "Post-transfer actions completed successfully."
            except Exception as e:
                return False, f"Post-transfer actions failed: {e}"

        return False, "Post-transfer actions failed due to unhandled recheck status."


def get_transfer_strategy(mode: str, config: Dict, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> TransferStrategy:
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
