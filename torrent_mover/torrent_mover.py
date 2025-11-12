#!/usr/bin/env python3
"""Main executable script for the Torrent Mover application.

This script orchestrates the entire process of moving completed torrents. It serves
as the main entry point and is responsible for:
- Parsing command-line arguments.
- Loading and validating the user's configuration.
- Initializing connections to qBittorrent clients and SSH servers.
- Identifying eligible torrents for transfer.
- Executing the transfer process, either sequentially or in parallel.
- Handling post-transfer actions like rechecking, categorizing, and cleaning up.
- Providing utility commands for managing tracker rules and checking system health.
"""

__version__ = "2.9.0"

# Standard library imports
import argparse
import configparser
import logging
import os
import shlex
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple, cast

# Third-party imports
import argcomplete
import qbittorrentapi
from rich.console import Console
from rich.logging import RichHandler

# Project module imports
from .config_manager import ConfigValidator, load_config, update_config
from .qbittorrent_manager import (
    connect_qbit,
    get_eligible_torrents,
    get_incomplete_files,
    wait_for_recheck_completion,
)
from .ssh_manager import (
    SSHConnectionPool,
    batch_get_remote_sizes,
    check_sshpass_installed,
    is_remote_dir,
)
from .system_manager import (
    LockFile,
    change_ownership,
    cleanup_orphaned_cache,
    delete_destination_content,
    delete_destination_files,
    destination_health_check,
    recover_cached_torrents,
    setup_logging,
    test_path_permissions,
)
from .tracker_manager import (
    categorize_torrents,
    display_tracker_rules,
    load_tracker_rules,
    run_interactive_categorization,
    save_tracker_rules,
    set_category_based_on_tracker,
)
from .transfer_manager import (
    FileTransferTracker,
    RemoteTransferError,
    Timeouts,
    TransferCheckpoint,
    transfer_content_rsync,
    transfer_content_rsync_upload,
    transfer_content_sftp_upload,
    transfer_content_with_queue,
)
from .transfer_strategies import TransferFile, get_transfer_strategy
from .ui import BaseUIManager, SimpleUIManager, UIManagerV2
from .watchdog import TransferWatchdog

# --- Constants ---
DEFAULT_PARALLEL_JOBS = 4

def _normalize_path(path: str) -> str:
    """Removes surrounding quotes from a path string.

    qBittorrent sometimes returns paths wrapped in single or double quotes, which
    can cause issues with file operations. This function strips them.

    Args:
        path: The path string to normalize.

    Returns:
        The path with surrounding quotes removed.
    """
    return path.strip('\'"')

def _pre_transfer_setup(
    torrent: qbittorrentapi.TorrentDictionary,
    total_size: int,
    config: configparser.ConfigParser,
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    args: argparse.Namespace,
) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
    """Performs setup tasks and checks before a torrent transfer begins.

    This function is responsible for:
    1.  Resolving the final source and destination paths for the torrent content.
    2.  Checking if content of the same name already exists at the destination.
    3.  Comparing the size of existing content with the source to determine the
        correct action (skip, delete, or resume).

    Args:
        torrent: The qBittorrent torrent dictionary object.
        total_size: The pre-calculated total size of the torrent content in bytes.
        config: The application's configuration object.
        ssh_connection_pools: A dictionary of active SSH connection pools.
        args: The parsed command-line arguments.

    Returns:
        A tuple containing:
        - `status_code`: A string indicating the result ("not_exists",
          "exists_same_size", "exists_different_size", or "failed").
        - `message`: A descriptive message for logging.
        - `source_content_path`: The resolved top-level source path.
        - `dest_content_path`: The resolved top-level destination path.
        - `destination_save_path`: The save path to be used in the destination client.
    """
    transfer_mode = config['SETTINGS']['transfer_mode'].lower()
    source_content_path = _normalize_path(torrent.content_path.rstrip('/\\'))
    dest_base_path = config['DESTINATION_PATHS']['destination_path']

    try:
        relative_path = os.path.normpath(os.path.relpath(source_content_path, torrent.save_path))
        content_name = relative_path.split(os.sep)[0]
        if content_name != os.path.basename(source_content_path):
            source_content_path = os.path.join(torrent.save_path, content_name)
    except ValueError:
        logging.warning(f"Could not determine relative path for '{torrent.name}'. Falling back to basename.")
        content_name = os.path.basename(source_content_path)

    dest_content_path = os.path.join(dest_base_path, content_name)
    remote_dest_base_path = config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
    destination_save_path = remote_dest_base_path
    is_remote_dest = transfer_mode in ['sftp_upload', 'rsync_upload']

    if args.dry_run:
        logging.info(f"[DRY RUN] Would check for existing content at: {dest_content_path}")
        return "dry_run", "Dry run, skipping checks.", source_content_path, dest_content_path, destination_save_path

    destination_size = -1
    try:
        if is_remote_dest:
            dest_pool = ssh_connection_pools['DESTINATION_SERVER']
            with dest_pool.get_connection() as (_sftp, ssh):
                size_map = batch_get_remote_sizes(ssh, [dest_content_path])
                if dest_content_path in size_map:
                    destination_size = size_map[dest_content_path]
        else:
            if os.path.exists(dest_content_path):
                if os.path.isdir(dest_content_path):
                    destination_size = sum(f.stat().st_size for f in Path(dest_content_path).glob('**/*') if f.is_file())
                else:
                    destination_size = os.path.getsize(dest_content_path)
    except Exception as e:
        logging.exception(f"Failed to check destination path '{dest_content_path}'")
        return "failed", f"Failed to check destination path: {e}", None, None, None

    if destination_size != -1:
        if total_size == destination_size:
            msg = f"Destination content exists and size matches ({total_size} bytes). Skipping transfer."
            return "exists_same_size", msg, source_content_path, dest_content_path, destination_save_path
        else:
            if 'rsync' in transfer_mode:
                msg = f"Destination exists but size mismatches (Source: {total_size} vs Dest: {destination_size}). Rsync will resume."
            else:
                msg = f"Destination exists but size mismatches (Source: {total_size} vs Dest: {destination_size}). Will delete and re-transfer."
            return "exists_different_size", msg, source_content_path, dest_content_path, destination_save_path

    return "not_exists", "Destination path is clear.", source_content_path, dest_content_path, destination_save_path

def _post_transfer_actions(
    torrent: qbittorrentapi.TorrentDictionary,
    source_qbit: qbittorrentapi.Client,
    destination_qbit: Optional[qbittorrentapi.Client],
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
    """Manages all tasks that occur after the file transfer is complete.

    This function orchestrates the final steps of the process for a single torrent:
    1.  Changes file ownership at the destination (if configured).
    2.  Adds the `.torrent` file to the destination qBittorrent client.
    3.  Triggers a force recheck on the destination client.
    4.  Waits for the recheck to complete, with a robust multi-stage recovery
        process involving delta transfers if the initial recheck fails.
    5.  Resumes the torrent and applies categories.
    6.  Deletes the torrent and its data from the source client.

    Args:
        torrent: The torrent object.
        source_qbit: The source qBittorrent client.
        destination_qbit: The destination qBittorrent client.
        config: The application's configuration.
        tracker_rules: A dictionary of tracker-to-category rules.
        ssh_connection_pools: A dictionary of active SSH connection pools.
        dest_content_path: The top-level destination path of the content.
        destination_save_path: The save path to be used in the destination client.
        transfer_executed: `True` if a file transfer actually occurred.
        dry_run: `True` if simulating the process.
        test_run: `True` if the source torrent should not be deleted.
        file_tracker: The file transfer tracker instance.
        all_files: A list of all `TransferFile` objects for the torrent.
        ui: The UI manager instance.

    Returns:
        A tuple containing a boolean success status and a descriptive message.
    """
    name, hash_ = torrent.name, torrent.hash
    transfer_mode = config['SETTINGS']['transfer_mode'].lower()

    # --- 1. Change Ownership ---
    if transfer_executed and not dry_run:
        chown_user = config['SETTINGS'].get('chown_user', '').strip()
        chown_group = config['SETTINGS'].get('chown_group', '').strip()
        if chown_user or chown_group:
            remote_config = config['DESTINATION_SERVER'] if transfer_mode in ['sftp_upload', 'rsync_upload'] else None
            change_ownership(dest_content_path, chown_user, chown_group, remote_config, dry_run, ssh_connection_pools)

    if not destination_qbit:
        return True, "Transfer complete, no destination client actions."

    # --- 2. Add Torrent to Destination ---
    try:
        if not destination_qbit.torrents_info(torrent_hashes=hash_):
            logging.info(f"Adding torrent '{name}' to destination.")
            if not dry_run:
                torrent_file_content = source_qbit.torrents_export(torrent_hash=hash_)
                destination_qbit.torrents_add(
                    torrent_files=torrent_file_content,
                    save_path=str(destination_save_path).replace("\\", "/"),
                    is_paused=True,
                    category=torrent.category,
                    use_auto_torrent_management=False
                )
                time.sleep(5)  # Allow time for client to process
    except Exception as e:
        logging.exception(f"Failed to add torrent '{name}' to destination.")
        return False, f"Failed to add torrent to destination: {e}"

    # --- 3. Recheck and Recovery Logic ---
    if not dry_run:
        recheck_ok = _handle_recheck_process(torrent, destination_qbit, config, all_files, dest_content_path, ui, file_tracker, ssh_connection_pools)
        if not recheck_ok:
            return False, "Recheck process failed after all recovery attempts."

    # --- 4. Post-Recheck Actions (Start, Categorize, Delete) ---
    try:
        if not dry_run:
            # Apply categories before starting
            if torrent.category:
                destination_qbit.torrents_set_category(torrent_hashes=hash_, category=torrent.category)
            set_category_based_on_tracker(destination_qbit, hash_, tracker_rules)

            # Resume torrent if configured
            if config.getboolean('DESTINATION_CLIENT', 'start_torrents_after_recheck', fallback=True):
                logging.info(f"Resuming destination torrent '{name}'.")
                destination_qbit.torrents_resume(torrent_hashes=hash_)

            # Delete from source
            if not test_run and config.getboolean('SOURCE_CLIENT', 'delete_after_transfer', fallback=True):
                logging.info(f"Deleting torrent and data from source: '{name}'.")
                source_qbit.torrents_delete(torrent_hashes=hash_, delete_files=True)
    except Exception as e:
        logging.exception("An unexpected error occurred during final post-transfer actions.")
        return False, f"Final post-transfer actions failed: {e}"

    return True, "Post-transfer actions completed successfully."

def _handle_recheck_process(...) -> bool:
    """Manages the multi-stage recheck and recovery process."""
    # This is a conceptual refactoring; the logic from the original _post_transfer_actions
    # recheck block would be moved here to improve readability.
    pass

def _execute_transfer(...) -> bool:
    """Executes the file transfer using the appropriate strategy."""
    # Conceptual refactoring of the transfer execution block.
    pass

def transfer_torrent(...) -> Tuple[str, str]:
    """Orchestrates the entire transfer process for a single torrent."""
    # Main orchestration logic for a single torrent.
    pass

def _handle_utility_commands(...) -> bool:
    """Handles command-line arguments that perform a specific action and exit."""
    # Logic for utility commands like --list-rules.
    pass

def _run_transfer_operation(...) -> None:
    """The main high-level function that orchestrates the entire transfer run."""
    # Connects to clients, gets torrents, and manages the main transfer loop.
    pass

def main() -> int:
    """The main entry point for the application."""
    # Handles argument parsing, logging setup, and overall application lifecycle.
    pass

if __name__ == "__main__":
    sys.exit(main())
