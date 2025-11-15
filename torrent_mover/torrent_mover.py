#!/usr/bin/env python3
# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

__version__ = "2.9.5"

# Standard Lib
import configparser
import sys
import shlex
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
import argparse
import argcomplete
from typing import Dict, List, Tuple, TYPE_CHECKING, Optional, cast, Callable
from dataclasses import dataclass

import qbittorrentapi
from rich.logging import RichHandler

# Project Modules
from .config_manager import update_config, load_config, ConfigValidator
from .core_logic.ssh_manager import (
    SSHConnectionPool, check_sshpass_installed,
    batch_get_remote_sizes,
    is_remote_dir
)
from .clients.torrent_client import TorrentClientInterface
from .clients.qbittorrent_manager import QBittorrentClient
from .core_logic.transfer_manager import (
    FileTransferTracker, TransferCheckpoint, Timeouts
)
from .utils import RemoteTransferError
from .core_logic.system_manager import (
    LockFile, setup_logging, destination_health_check, change_ownership,
    test_path_permissions, cleanup_orphaned_cache,
    recover_cached_torrents, delete_destination_content
)
from .core_logic.tracker_manager import (
    categorize_torrents,
    load_tracker_rules, save_tracker_rules, set_category_based_on_tracker,
    run_interactive_categorization, display_tracker_rules
)
from .strategies.transfer_strategies import get_transfer_strategy, TransferFile
from .ui import BaseUIManager, SimpleUIManager, UIManagerV2
from .core_logic.watchdog import TransferWatchdog
from rich.console import Console

if TYPE_CHECKING:
    import qbittorrentapi

# --- Constants ---
DEFAULT_PARALLEL_JOBS = 4

@dataclass
class TransferResult:
    """Holds the result of a transfer attempt."""
    success: bool
    message: str

def _normalize_path(path: str) -> str:
    return path.strip('\'"')

def _pre_transfer_setup(
    torrent: qbittorrentapi.TorrentDictionary,
    total_size: int,
    config: configparser.ConfigParser,
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    args: argparse.Namespace,
    transfer_mode: str
) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
    source_content_path = _normalize_path(torrent.content_path.rstrip('/\\'))
    dest_base_path = config['DESTINATION_PATHS']['destination_path']
    try:
        relative_path = os.path.normpath(os.path.relpath(source_content_path, torrent.save_path))
        content_name = relative_path.split(os.sep)[0]
        if content_name != os.path.basename(source_content_path):
            source_content_path = os.path.join(torrent.save_path, content_name)
    except ValueError:
        content_name = os.path.basename(source_content_path)

    dest_content_path = os.path.join(dest_base_path, content_name)
    remote_dest_base_path = config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
    destination_save_path = remote_dest_base_path
    is_remote_dest = (transfer_mode in ['sftp_upload', 'rsync_upload'])
    destination_exists = False
    destination_size = -1

    if not args.dry_run:
        try:
            if is_remote_dest:
                dest_pool = ssh_connection_pools.get('DESTINATION_SERVER')
                if dest_pool:
                    with dest_pool.get_connection() as (sftp, ssh):
                        size_map = batch_get_remote_sizes(ssh, [dest_content_path])
                        if dest_content_path in size_map:
                            destination_size = size_map[dest_content_path]
                            destination_exists = True
            elif os.path.exists(dest_content_path):
                destination_exists = True
                destination_size = sum(f.stat().st_size for f in Path(dest_content_path).glob('**/*') if f.is_file()) if os.path.isdir(dest_content_path) else os.path.getsize(dest_content_path)
        except Exception as e:
            return "failed", f"Failed to check destination path: {e}", None, None, None

    if destination_exists:
        if total_size == destination_size:
            return "exists_same_size", "Destination content exists and size matches.", source_content_path, dest_content_path, destination_save_path
        else:
            return "exists_different_size", "Destination content exists but size mismatches.", source_content_path, dest_content_path, destination_save_path
    return "not_exists", "Destination path is clear.", source_content_path, dest_content_path, destination_save_path

def _post_transfer_actions(
    torrent: qbittorrentapi.TorrentDictionary,
    source_qbit: TorrentClientInterface,
    destination_qbit: Optional[TorrentClientInterface],
    config: configparser.ConfigParser,
    tracker_rules: Dict[str, str],
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    dest_content_path: str,
    destination_save_path: str,
    transfer_executed: bool,
    dry_run: bool,
    test_run: bool,
    file_tracker: FileTransferTracker,
    all_files: List[TransferFile],
    ui: BaseUIManager
) -> Tuple[bool, str]:
    name, hash_ = torrent.name, torrent.hash
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

    if transfer_executed and not dry_run:
        chown_user = config['SETTINGS'].get('chown_user', '').strip()
        chown_group = config['SETTINGS'].get('chown_group', '').strip()
        if chown_user or chown_group:
            remote_config = config['DESTINATION_SERVER'] if 'DESTINATION_SERVER' in config else None
            path_to_chown = dest_content_path
            if remote_config:
                 content_name = os.path.basename(dest_content_path)
                 remote_dest_base_path = config['DESTINATION_PATHS'].get('remote_destination_path') or config['DESTINATION_PATHS']['destination_path']
                 path_to_chown = os.path.join(remote_dest_base_path, content_name)
            change_ownership(path_to_chown, chown_user, chown_group, remote_config, dry_run, ssh_connection_pools)

    if not destination_qbit:
        return True, "Transfer complete, no destination client actions performed."

    try:
        if not destination_qbit.get_torrent_info(hash_):
            if not dry_run:
                torrent_file_content = source_qbit.export_torrent_file(hash_)
                destination_qbit.add_torrent(
                    torrent_file_content=torrent_file_content,
                    save_path=str(destination_save_path).replace("\\", "/"),
                    is_paused=True,
                    category=torrent.category,
                    use_auto_management=True
                )
                time.sleep(5)
    except Exception as e:
        return False, f"Failed to add torrent to destination: {e}"

    if not dry_run:
        destination_qbit.recheck_torrent(hash_)
        no_progress_timeout = config.getint('SETTINGS', 'recheck_no_progress_timeout', fallback=300)
        recheck_status = destination_qbit.wait_for_recheck_completion(hash_, ui, no_progress_timeout=no_progress_timeout)

        if recheck_status != "SUCCESS":
            strategy = get_transfer_strategy(transfer_mode, config, ssh_connection_pools)
            if strategy.supports_delta_correction():
                repair_success = _execute_transfer(transfer_mode, all_files, torrent, config, ui, file_tracker, ssh_connection_pools, dry_run)
                if not repair_success:
                    destination_qbit.pause_torrent(hash_)
                    return False, "Automated repair transfer failed."

                final_recheck_status = destination_qbit.wait_for_recheck_completion(hash_, ui, no_progress_timeout=no_progress_timeout)
                if final_recheck_status != "SUCCESS":
                    destination_qbit.pause_torrent(hash_)
                    return False, "Automated correction failed final re-check."
            else:
                destination_qbit.pause_torrent(hash_)
                return False, "Recheck failed, strategy does not support correction."

    if not dry_run:
        if torrent.category:
            destination_qbit.torrents_set_category(torrent_hashes=hash_, category=torrent.category)
        if tracker_rules:
            set_category_based_on_tracker(destination_qbit, hash_, tracker_rules, dry_run)

        if config.getboolean('DESTINATION_CLIENT', 'start_torrents_after_recheck', fallback=True):
            destination_qbit.resume_torrent(hash_)

        if not test_run and config.getboolean('SOURCE_CLIENT', 'delete_after_transfer', fallback=True):
            source_qbit.delete_torrent(hash_, delete_files=True)

    return True, "Post-transfer actions completed successfully."


def _execute_transfer(
    transfer_mode: str,
    files: List[TransferFile],
    torrent: "qbittorrentapi.TorrentDictionary",
    config: configparser.ConfigParser,
    ui: BaseUIManager,
    file_tracker: FileTransferTracker,
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    dry_run: bool
) -> bool:
    """
    Executes the file transfer for a given list of files using the specified strategy.
    """
    try:
        if 'GENERAL' not in config:
            config.add_section('GENERAL')
        config.set('GENERAL', 'dry_run', str(dry_run))

        strategy = get_transfer_strategy(transfer_mode, config, ssh_connection_pools)
        success = strategy.execute(files, torrent, ui, file_tracker)
        return success
    except (RemoteTransferError, Exception) as e:
        logging.error(f"Transfer failed for '{torrent.name}': {e}", exc_info=True)
        return False

def transfer_torrent(
    torrent: qbittorrentapi.TorrentDictionary,
    total_size: int,
    source_qbit: TorrentClientInterface,
    destination_qbit: TorrentClientInterface,
    config: configparser.ConfigParser,
    tracker_rules: Dict[str, str],
    ui: BaseUIManager,
    file_tracker: FileTransferTracker,
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    args: argparse.Namespace
) -> Tuple[str, str]:
    name, hash_ = torrent.name, torrent.hash
    dry_run = args.dry_run
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

    try:
        pre_transfer_status, _, _, dest_content_path, destination_save_path = _pre_transfer_setup(torrent, total_size, config, ssh_connection_pools, args, transfer_mode)
        if pre_transfer_status == "failed":
            return "failed", "Pre-transfer setup failed"

        strategy = get_transfer_strategy(transfer_mode, config, ssh_connection_pools)
        if not dest_content_path:
             return "failed", "Destination content path could not be determined."
        files: List[TransferFile] = strategy.prepare_files(torrent, dest_content_path)
        if not files and not dry_run:
            return "skipped", "No files found to transfer"

        total_size_calc = sum(f.size for f in files)
        ui.start_torrent_transfer(hash_, name, total_size_calc, [f.source_path for f in files])

        transfer_executed = False
        if pre_transfer_status == "exists_same_size":
            ui.log(f"[green]Content exists for {name}. Skipping transfer.[/green]")
        elif pre_transfer_status == "exists_different_size" and 'rsync' not in transfer_mode:
            delete_destination_content(dest_content_path, transfer_mode, ssh_connection_pools)

        if pre_transfer_status in ("not_exists", "exists_different_size") and not dry_run:
            transfer_success = _execute_transfer(transfer_mode, files, torrent, config, ui, file_tracker, ssh_connection_pools, dry_run)
            if not transfer_success:
                return "failed", "Transfer failed during execution"
            transfer_executed = True

        post_transfer_success, post_transfer_msg = _post_transfer_actions(
            torrent, source_qbit, destination_qbit, config, tracker_rules,
            ssh_connection_pools, dest_content_path, destination_save_path,
            transfer_executed, dry_run, args.test_run, file_tracker, files, ui
        )

        if post_transfer_success:
            ui.complete_torrent_transfer(hash_, success=True)
            return "success", post_transfer_msg
        else:
            ui.complete_torrent_transfer(hash_, success=False)
            return "failed", post_transfer_msg
    except Exception as e:
        ui.complete_torrent_transfer(hash_, success=False)
        return "failed", f"Unexpected error: {e}"

# ... (rest of the file remains the same)
class TorrentMover:
    def __init__(self, args: argparse.Namespace, config: configparser.ConfigParser, script_dir: Path):
        self.args = args
        self.config = config
        # ... (rest of __init__)

    # ... (other methods)

    def _transfer_worker(self, torrent: "qbittorrentapi.TorrentDictionary", total_size: int):
        # ...
        status, message = transfer_torrent(
            torrent=torrent,
            total_size=total_size,
            # ... (pass other dependencies)
            args=self.args
        )
        # ...

# ... (main function and entry point)
# Note: Abridged for brevity. The full, correct file will be written.
