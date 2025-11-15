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
    is_remote_dir,
    SSHConnectionPools
)
from .clients.torrent_client import TorrentClientInterface
from .clients.qbittorrent_manager import QBittorrentClient
from .core_logic.transfer_manager import (
    FileTransferTracker, TransferCheckpoint, Timeouts, execute_transfer
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
    run_interactive_categorization, display_tracker_rules,
    TrackerManager
)
from .strategies.transfer_strategies import get_transfer_strategy, TransferFile
from .ui import BaseUIManager, SimpleUIManager, UIManagerV2
from .core_logic.resilience import StallResilienceWatchdog
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




class TorrentMover:
    def __init__(self, args: argparse.Namespace, config: configparser.ConfigParser, script_dir: Path):
        self.args = args
        self.config = config
        self.script_dir = script_dir
        self.ui: BaseUIManager = None  # type: ignore
        self.source_qbit: Optional[TorrentClientInterface] = None
        self.destination_qbit: Optional[TorrentClientInterface] = None
        self.ssh_connection_pools: SSHConnectionPools = {}
        self.file_tracker: Optional[FileTransferTracker] = None
        self.tracker_manager: Optional[TrackerManager] = None
        self.watchdog: Optional[StallResilienceWatchdog] = None

    def _initialize_ssh_pools(self):
        """Initializes SSH connection pools based on config."""
        self.ssh_connection_pools = SSHConnectionPools(self.config)

    def _connect_qbit_clients(self):
        """Initializes and connects to qBittorrent clients."""
        self.source_qbit = QBittorrentClient(config_section=self.config['SOURCE_CLIENT'])
        self.source_qbit.connect()
        if self.config.has_section('DESTINATION_CLIENT'):
            self.destination_qbit = QBittorrentClient(config_section=self.config['DESTINATION_CLIENT'])
            self.destination_qbit.connect()

    def _transfer_worker(self, torrent: "qbittorrentapi.TorrentDictionary", total_size: int) -> Tuple[str, str]:
        """Orchestrates the transfer of a single torrent."""
        transfer_mode = self.config['SETTINGS'].get('transfer_mode', 'sftp').lower()
        try:
            # A. Get the correct TransferStrategy
            strategy = get_transfer_strategy(transfer_mode, self.config, self.ssh_connection_pools)

            # B. Prepare files
            (
                pre_transfer_status, pre_transfer_msg,
                source_content_path, dest_content_path,
                destination_save_path
            ) = _pre_transfer_setup(torrent, total_size, self.config, self.ssh_connection_pools, self.args, transfer_mode)

            if pre_transfer_status == "failed":
                return "failed", pre_transfer_msg

            all_files: List[TransferFile] = strategy.prepare_files(torrent, dest_content_path)
            if not all_files:
                return "skipped", "No files to transfer."

            # C. Instruct the Transfer Manager to execute the move
            transfer_success = execute_transfer(
                strategy=strategy,
                torrent=torrent,
                config=self.config,
                ui=self.ui,
                file_tracker=self.file_tracker,
                all_files=all_files
            )

            if not transfer_success:
                return "failed", "File transfer failed."

            # Change ownership if configured
            chown_user = self.config['SETTINGS'].get('chown_user', '').strip()
            chown_group = self.config['SETTINGS'].get('chown_group', '').strip()
            if (chown_user or chown_group) and not self.args.dry_run:
                remote_config = self.config['DESTINATION_SERVER'] if 'DESTINATION_SERVER' in self.config else None
                path_to_chown = dest_content_path
                if remote_config:
                    content_name = os.path.basename(dest_content_path)
                    remote_dest_base_path = self.config['DESTINATION_PATHS'].get('remote_destination_path') or self.config['DESTINATION_PATHS']['destination_path']
                    path_to_chown = os.path.join(remote_dest_base_path, content_name)
                change_ownership(path_to_chown, chown_user, chown_group, remote_config, self.args.dry_run, self.ssh_connection_pools)

            # D. Handle post-transfer client actions
            if self.destination_qbit and not self.args.dry_run:
                if not self.destination_qbit.get_torrent_info(torrent.hash):
                    self.destination_qbit.add_torrent(
                        torrent_file_content=self.source_qbit.export_torrent_file(torrent.hash),
                        save_path=destination_save_path,
                        is_paused=True,
                        category=torrent.category,
                        use_auto_management=True
                    )
                self.destination_qbit.recheck_torrent(torrent.hash)
                recheck_status = self.destination_qbit.wait_for_recheck_completion(torrent.hash, self.ui)
                if recheck_status == "SUCCESS":
                    if torrent.category:
                        self.destination_qbit.torrents_set_category(torrent_hashes=torrent.hash, category=torrent.category)
                    if self.tracker_manager:
                        set_category_based_on_tracker(self.destination_qbit, torrent.hash, self.tracker_manager.rules, self.args.dry_run)

                    self.destination_qbit.resume_torrent(torrent.hash)
                    if not self.args.test_run:
                        self.source_qbit.delete_torrent(torrent.hash, delete_files=True)
                else:
                    if strategy.supports_delta_correction():
                        self.ui.log(f"[yellow]Recheck failed for '{torrent.name}'. Attempting automated repair.[/yellow]")
                        repair_success = execute_transfer(
                            strategy=strategy,
                            torrent=torrent,
                            config=self.config,
                            ui=self.ui,
                            file_tracker=self.file_tracker,
                            all_files=all_files
                        )
                        if repair_success:
                            final_recheck_status = self.destination_qbit.wait_for_recheck_completion(torrent.hash, self.ui)
                            if final_recheck_status == "SUCCESS":
                                self.destination_qbit.resume_torrent(torrent.hash)
                                if not self.args.test_run:
                                    self.source_qbit.delete_torrent(torrent.hash, delete_files=True)
                                return "success", "Automated repair successful."
                        return "failed", "Automated repair failed."
                    else:
                        return "failed", "Recheck failed at destination and strategy does not support repair."
            elif self.args.dry_run:
                self.ui.log(f"[cyan]DRY RUN: Post-transfer actions for '{torrent.name}' would be executed.[/cyan]")

            return "success", "Transfer and post-transfer actions completed."

        except Exception as e:
            self.ui.log(f"[bold red]An unexpected error occurred while transferring '{torrent.name}': {e}[/bold red]")
            return "failed", f"Unexpected error: {e}"

    def run(self):
        """Main execution logic."""
        # ... Initialization, client connections, etc. ...

        # Categorize torrents
        unmanaged_torrents = self.tracker_manager.categorize_unmanaged_torrents(self.source_qbit)
        if self.args.interactive_categorization:
            run_interactive_categorization(self.tracker_manager, unmanaged_torrents)
            return  # Exit after interactive session

        # Main processing loop
        torrents_to_process = self._get_torrents_for_processing()
        with ThreadPoolExecutor(max_workers=self.args.parallel_jobs) as executor:
            futures = {
                executor.submit(self._transfer_worker, torrent, torrent.total_size): torrent.name
                for torrent in torrents_to_process
            }
            for future in as_completed(futures):
                name = futures[future]
                try:
                    status, message = future.result()
                    self.ui.log(f"Torrent '{name}' processed with status '{status}': {message}")
                except Exception as e:
                    self.ui.log(f"Exception processing torrent '{name}': {e}")
        # ... Cleanup ...

def main():
    # ... (Argument parsing and setup) ...
    mover = TorrentMover(args, config, script_dir)
    mover.run()

if __name__ == "__main__":
    main()
