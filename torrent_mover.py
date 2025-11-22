#!/usr/bin/env python3
# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

__version__ = "2.13.0"

# Standard Lib
import configparser
import sys
import shlex
import logging
import traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
import argparse
import argcomplete
from typing import Dict, List, Tuple, TYPE_CHECKING, Optional, cast, Callable, Any
import typing
from dataclasses import dataclass

import qbittorrentapi
from rich.logging import RichHandler

# Project Modules
from config_manager import update_config, ConfigManager
from ssh_manager import (
    SSHConnectionPool, check_sshpass_installed,
    _get_all_files_recursive, batch_get_remote_sizes,
    is_remote_dir
)
from clients.factory import get_client
from clients.base import TorrentClient
from transfer_manager import (
    FileTransferTracker, TransferCheckpoint, transfer_content_rsync,
    transfer_content_with_queue, transfer_content_sftp_upload, Timeouts,
    transfer_content_rsync_upload, RemoteTransferError
)
from system_manager import (
    LockFile, setup_logging, destination_health_check, change_ownership,
    test_path_permissions, cleanup_orphaned_cache,
    recover_cached_torrents, delete_destination_content,
    delete_destination_files
)
from tracker_manager import (
    categorize_torrents,
    load_tracker_rules, save_tracker_rules, set_category_based_on_tracker,
    run_interactive_categorization, display_tracker_rules, get_category_from_rules
)
from transfer_strategies import get_transfer_strategy, TransferFile
from ui import BaseUIManager, SimpleUIManager, UIManagerV2
from watchdog import TransferWatchdog
from rich.logging import RichHandler
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

# --- Module-Level Functions ---

def _normalize_path(path: str) -> str:
    """Remove surrounding quotes from paths returned by qBittorrent.

    qBittorrent sometimes returns paths wrapped in quotes like '/path/to/file'
    which causes issues with rsync and other operations.

    Args:
        path: The path string to normalize

    Returns:
        The path with surrounding quotes removed
    """
    return path.strip('\'"')


def _pre_transfer_setup(
    torrent: qbittorrentapi.TorrentDictionary,
    total_size: int, # Pass in the pre-calculated total_size
    config: configparser.ConfigParser,
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    args: argparse.Namespace,
    transfer_mode: str
) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
    """Performs setup tasks before a torrent transfer begins.

    This function is responsible for:
    1.  Resolving source and destination paths for the torrent content.
    2.  Checking if content already exists at the destination and comparing its
        size with the source.

    Args:
        torrent: The qBittorrent torrent dictionary object.
        total_size: The pre-calculated total size of the torrent content in bytes.
        config: The application's ConfigParser object.
        ssh_connection_pools: Dictionary of SSH connection pools.
        args: The command-line arguments namespace.

    Returns:
        A tuple containing:
        -   status_code (str): One of "not_exists", "exists_same_size",
            "exists_different_size", or "failed".
        -   message (str): A descriptive message for logging.
        -   source_content_path (Optional[str]): The top-level source path.
        -   dest_content_path (Optional[str]): The top-level destination path.
        -   destination_save_path (Optional[str]): The save path for the torrent
            client.
    """
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    source_content_path = _normalize_path(torrent.content_path.rstrip('/\\'))
    dest_base_path = config['DESTINATION_PATHS']['destination_path']
    # Determine the correct content name, preserving the root folder
    # for single-file torrents that are in a directory.
    try:
        # Find path of content relative to the torrent's save path
        # e.g., "My.Movie.2023/My.Movie.2023.mkv"
        relative_path = os.path.normpath(os.path.relpath(source_content_path, torrent.save_path))
        # The content name is the first part of that relative path
        # e.g., "My.Movie.2023"
        content_name = relative_path.split(os.sep)[0]

        # If the determined content_name is different from the basename of the
        # original content_path, it means we have a file inside a folder.
        # We must update the source_content_path to point to the FOLDER,
        # not the file, to ensure the whole folder is moved.
        original_basename = os.path.basename(source_content_path)
        if content_name != original_basename:
            source_content_path = os.path.join(torrent.save_path, content_name)
            logging.debug(
                f"Adjusted source path for single-file torrent in folder. "
                f"New source: '{source_content_path}'"
            )

    except ValueError:
        # This can happen if save_path and content_path are on different drives (Windows)
        # Fallback to the original, less accurate method
        logging.warning(f"Could not determine relative path for '{torrent.name}'. Falling back to basename.")
        content_name = os.path.basename(source_content_path)

    dest_content_path = os.path.join(dest_base_path, content_name)
    remote_dest_base_path = config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
    destination_save_path = remote_dest_base_path
    is_remote_dest = (transfer_mode in ['sftp_upload', 'rsync_upload'])

    # --- 1. Check Destination Path ---
    destination_exists = False
    destination_size = -1

    if args.dry_run:
        logging.info(f"[DRY RUN] Would check for existing content at: {dest_content_path}")
    else:
        try:
            if is_remote_dest:
                # Remote check for 'sftp_upload'
                dest_server_section = 'DESTINATION_SERVER' # Hardcode to the correct section name
                dest_pool = ssh_connection_pools.get(dest_server_section)
                if not dest_pool:
                    return "failed", f"SSH pool '{dest_server_section}' not found.", None, None, None

                logging.debug(f"Checking remote destination size for: {dest_content_path}")
                with dest_pool.get_connection() as (sftp, ssh):
                    # Use batch_get_remote_sizes for a single path; it's robust.
                    size_map = batch_get_remote_sizes(ssh, [dest_content_path])
                    if dest_content_path in size_map:
                        destination_size = size_map[dest_content_path]
                        destination_exists = True
            else:
                # Local check for 'sftp' or 'rsync'
                logging.debug(f"Checking local destination size for: {dest_content_path}")
                if os.path.exists(dest_content_path):
                    destination_exists = True
                    if os.path.isdir(dest_content_path):
                        # Calculate local directory size (this can be slow, but necessary)
                        destination_size = sum(f.stat().st_size for f in Path(dest_content_path).glob('**/*') if f.is_file())
                    else:
                        destination_size = os.path.getsize(dest_content_path)

        except Exception as e:
            logging.exception(f"Failed to check destination path '{dest_content_path}'")
            return "failed", f"Failed to check destination path: {e}", None, None, None

    # --- 2. Compare Sizes and Determine Status ---
    status_code = "not_exists"
    status_message = "Destination path is clear."

    if destination_exists:
        if total_size == destination_size:
            status_code = "exists_same_size"
            status_message = f"Destination content exists and size matches source ({total_size} bytes). Skipping transfer."
            logging.warning(status_message)
        else:
            status_code = "exists_different_size"
            if 'rsync' in transfer_mode:
                status_message = (
                    f"Destination content exists but size mismatches "
                    f"(Source: {total_size} vs Dest: {destination_size}). "
                    f"Rsync mode: Will resume transfer."
                )
            else:
                status_message = (
                    f"Destination content exists but size mismatches "
                    f"(Source: {total_size} vs Dest: {destination_size}). "
                    f"Will delete and re-transfer."
                )
            logging.warning(status_message)
    else:
        logging.debug(f"Destination path does not exist: {dest_content_path}")

    return status_code, status_message, source_content_path, dest_content_path, destination_save_path

from transfer_strategies import get_transfer_strategy, TransferFile

def _post_transfer_actions(
    torrent: qbittorrentapi.TorrentDictionary,
    source_client: Optional[TorrentClient],
    destination_client: Optional[TorrentClient],
    config: configparser.ConfigParser,
    tracker_rules: Dict[str, str],
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    dest_content_path: str,
    destination_save_path: str,
    transfer_executed: bool,
    dry_run: bool,
    test_run: bool,
    file_tracker: FileTransferTracker,
    transfer_mode: str,
    all_files: List[TransferFile],
    ui: BaseUIManager,
    # --- Add these new arguments ---
    log_transfer: Callable,
    _update_transfer_progress: Callable
) -> Tuple[bool, str]:
    """Manages tasks after the file transfer is complete.

    This function is responsible for:
    1.  Changing file ownership at the destination (if configured).
    2.  Adding the torrent to the destination client.
    3.  Forcing a recheck of the torrent data on the destination.
    4.  Waiting for the recheck to complete and handling failures.
    5.  Starting the torrent and applying tracker-based categories.
    6.  Deleting the torrent and its data from the source client.

    Args:
        torrent: The torrent object.
        source_client: The source TorrentClient.
        destination_client: The destination TorrentClient.
        config: The application's configuration.
        tracker_rules: A dictionary of tracker-to-category rules.
        ssh_connection_pools: Dictionary of SSH connection pools.
        dest_content_path: The top-level destination path of the content.
        destination_save_path: The save path for the torrent client.
        transfer_executed: A boolean indicating if a file transfer actually
            occurred. Some actions (like chown) are skipped if this is False.
        dry_run: If True, simulates actions without making changes.
        test_run: If True, skips deleting the torrent from the source.

    Returns:
        A tuple containing a boolean success status and a descriptive message.
    """
    name, hash_ = torrent.name, torrent.hash
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

    # --- 1. Change Ownership (if transfer was executed) ---
    if transfer_executed and not dry_run:
        chown_user = config['SETTINGS'].get('chown_user', '').strip()
        chown_group = config['SETTINGS'].get('chown_group', '').strip()
        if chown_user or chown_group:
            remote_config = None
            path_to_chown = dest_content_path

            # New logic: If a DESTINATION_SERVER is defined, *always*
            # attempt the chown on that server using the remote path.
            if 'DESTINATION_SERVER' in config and config.has_section('DESTINATION_SERVER'):
                try:
                    remote_config = config['DESTINATION_SERVER']
                    content_name = os.path.basename(dest_content_path)
                    remote_dest_base_path = config['DESTINATION_PATHS'].get('remote_destination_path')

                    if not remote_dest_base_path:
                        raise ValueError("`remote_destination_path` is not defined in config under [DESTINATION_PATHS]")

                    # Construct the full *remote* path for chown
                    path_to_chown = os.path.join(remote_dest_base_path, content_name)
                    logging.info(f"DESTINATION_SERVER defined. Will run chown on remote path: {path_to_chown}")
                except Exception as e:
                    logging.error(f"Error determining remote chown path, falling back to local. Error: {e}")
                    remote_config = None
                    path_to_chown = dest_content_path

            if not change_ownership(path_to_chown, chown_user, chown_group, remote_config, dry_run, ssh_connection_pools):
                msg = f"PROVISIONING ERROR: Ownership change failed for {dest_content_path}. Source preserved."
                logging.error(msg)
                return False, msg

    if not destination_client:
        logging.warning("No destination client provided. Skipping post-transfer client actions.")
        # If no destination client, we can't do anything else, so just return success.
        return True, "Transfer complete, no destination client actions performed."

    destination_save_path_str = str(destination_save_path).replace("\\", "/")

    # --- 2. Add Torrent to Destination (if not already there) ---
    try:
        dest_torrent = destination_client.get_torrent_info(torrent_hash=hash_)
        if not dest_torrent:
            logging.info(f"Torrent {name} not found on destination. Adding it.")
            if not dry_run:
                logging.debug(f"Exporting .torrent file for {name}")
                try:
                    torrent_file_content = source_client.export_torrent(torrent_hash=hash_)
                except Exception as e:
                    logging.error(f"Failed to export .torrent file for {name}: {e}")
                    return False, f"Failed to export .torrent file: {e}"

                logging.info(f"CLIENT: Adding torrent to Destination (AMM=True, paused) with save path '{destination_save_path_str}': {name}")
                try:
                    destination_client.add_torrent(
                        torrent_files=torrent_file_content,
                        save_path=destination_save_path_str,
                        is_paused=True,
                        category=torrent.category
                    )
                    time.sleep(5) # Give client time to add it
                except Exception as e:
                    logging.error(f"Failed to add torrent to destination: {e}")
                    return False, f"Failed to add torrent to destination: {e}"
            else:
                logging.info(f"[DRY RUN] Would add torrent to Destination (paused) with save path '{destination_save_path_str}': {name}")
        else:
            logging.info(f"Torrent {name} already exists on destination. Proceeding to recheck.")

    except Exception as e:
        logging.exception(f"Failed to add torrent {name} to destination")
        return False, f"Failed to add torrent to destination: {e}"

    # --- 3. Force Recheck ---
    if not dry_run:
        logging.info(f"CLIENT: Triggering force recheck on Destination for: {name}")
        try:
            destination_client.recheck_torrent(torrent_hash=hash_)
        except Exception as e:
            msg = f"Failed to start recheck: {e}"
            logging.error(msg)
            return False, msg
    else:
        logging.info(f"[DRY RUN] Would trigger force recheck on Destination for: {name}")

    # --- 4. Wait for Recheck and Handle Failure ---
    recheck_status = "SUCCESS"  # Default for dry run
    if not dry_run:
        logging.info(f"Waiting for destination re-check to complete for {torrent.name}...")
        recheck_stuck_timeout = config.getint('SETTINGS', 'recheck_stuck_timeout', fallback=60)
        recheck_stopped_timeout = config.getint('SETTINGS', 'recheck_stopped_timeout', fallback=15)
        recheck_status = destination_client.wait_for_recheck(
            torrent_hash=torrent.hash,
            ui_manager=ui,
            stuck_timeout=recheck_stuck_timeout,
            stopped_timeout=recheck_stopped_timeout,
            dry_run=dry_run
        )

    # (This logic goes *after* wait_for_recheck_completion and *before* the success block)
    # --- START MODIFICATION ---

    if recheck_status == "FAILED_FINAL_REVIEW":
        manual_review_category = config['SETTINGS'].get('manual_review_category')
        logging.error("Torrent failed final verification. Preserving source and categorizing for review.")
        ui.log(f"[bold red]Verification Failed: {name}. Flagging for review.[/bold red]")

        if manual_review_category and not dry_run:
            try:
                destination_client.set_category(torrent_hash=hash_, category=manual_review_category)
            except Exception as e:
                logging.warning(f"Failed to set manual review category: {e}")

        if not dry_run:
            try:
                destination_client.pause_torrent(torrent_hash=hash_)
            except Exception as e:
                logging.warning(f"Failed to pause torrent {name}: {e}")

        return False, "Verification failed. Source preserved for manual review."

    if recheck_status == "FAILED_STUCK":
        # This is the new "Stuck" failure from Directive 2, Scenario B
        # We do NOT trigger a delta-sync. Log, pause, and return failure.
        logging.error(f"Recheck FAILED for {name} with status: {recheck_status} (Stuck).")
        ui.log(f"[bold red]Recheck FAILED for {name}: {recheck_status}[/bold red]")
        if not dry_run:
            try:
                destination_client.pause_torrent(torrent_hash=hash_)
            except Exception as e:
                logging.warning(f"Failed to pause torrent {name} after stuck recheck failure: {e}")
        return False, "Recheck failed (stuck), no delta-sync attempted."

    elif recheck_status == "FAILED_STATE":
        # This is the "Explicit Failure" from Directive 2, Scenario A
        # This is the *only* case that triggers the delta-sync repair.
        logging.error(f"Recheck FAILED for {name} with status: {recheck_status} (Stopped/Error).")
        ui.log(f"[bold red]Recheck FAILED for {name}: {recheck_status}[/bold red]")

        # Directive 3: Automated Error Correction
        strategy = get_transfer_strategy(transfer_mode, config, ssh_connection_pools)

        if not dry_run and strategy.supports_delta_correction():
            logging.warning(f"Strategy '{transfer_mode}' supports delta-correction. Attempting automated repair...")
            ui.log(f"[yellow]Attempting automated repair for {name}...[/yellow]")

            # Re-run the transfer to repair
            repair_success = _execute_transfer(
                transfer_mode, all_files, torrent, sum(f.size for f in all_files), config,
                ui, file_tracker, ssh_connection_pools, dry_run,
                log_transfer, _update_transfer_progress
            )

            if not repair_success:
                logging.error(f"Automated repair (delta transfer) FAILED for {name}.")
                ui.log(f"[bold red]Automated repair FAILED for {name}.[/bold red]")
                # Pause torrent, do not delete, return failure
                try:
                    destination_client.pause_torrent(torrent_hash=hash_)
                except Exception as e:
                    logging.warning(f"Failed to pause torrent {name} after repair failure: {e}")
                return False, "Automated repair transfer failed."

            logging.info(f"Automated repair transfer complete. Triggering re-check for {name}...")
            destination_client.recheck_torrent(torrent_hash=hash_)
            time.sleep(1) # Allow API to process

            # Run re-check a final time
            logging.info(f"Automated repair complete. Triggering final re-check for {name}...")
            ui.log(f"Repair complete. Final re-check for {name}...")

            # Read new config values for the *second* recheck
            recheck_stuck_timeout = config.getint('SETTINGS', 'recheck_stuck_timeout', fallback=60)
            recheck_stopped_timeout = config.getint('SETTINGS', 'recheck_stopped_timeout', fallback=15)

            final_recheck_status = destination_client.wait_for_recheck(
                torrent_hash=torrent.hash,
                ui_manager=ui,
                stuck_timeout=recheck_stuck_timeout,
                stopped_timeout=recheck_stopped_timeout,
                dry_run=dry_run
            )

            if final_recheck_status == "SUCCESS":
                logging.info(f"Final re-check successful for {name} after repair.")
                recheck_status = "SUCCESS" # Fall through to the success block
            else:
                logging.error(f"Final re-check FAILED for {name} after repair (Status: {final_recheck_status}).")
                ui.log(f"[bold red]Final re-check FAILED for {name} after repair.[/bold red]")
                # Pause torrent, do not delete, return failure
                try:
                    destination_client.pause_torrent(torrent_hash=hash_)
                except Exception as e:
                    logging.warning(f"Failed to pause torrent {name} after final recheck failure: {e}")
                return False, "Automated correction failed final re-check."

        elif not strategy.supports_delta_correction():
            logging.warning(f"Recheck failed and strategy '{transfer_mode}' does not support delta-correction.")
            ui.log(f"[yellow]Recheck failed for {name}. No auto-repair possible.[/yellow]")
            # Pause torrent, do not delete, return failure
            if not dry_run:
                try:
                    destination_client.pause_torrent(torrent_hash=hash_)
                except Exception as e:
                    logging.warning(f"Failed to pause torrent {name} after recheck failure: {e}")
            return False, "Recheck failed, strategy does not support correction."

        else: # Is dry_run
            logging.info(f"[DRY RUN] Would attempt repair or pause torrent {name}.")
            # We return False here to stop the process, as recheck "failed" in dry run
            return False, "Dry run: Recheck failed."

    # --- END MODIFICATION ---

    # --- 5. Post-Recheck Actions (Start, Categorize, Delete Source) ---
    if recheck_status == "SUCCESS":
        logging.info("Destination re-check successful.")
        try:
            # AI-CONTEXT: Category Precedence
            # Tracker Rules > Source Category.
            # We apply the source category first, then override it with tracker rules if a match is found.

            # 1. Apply Category FIRST (before starting)
            if torrent.category:
                try:
                    logging.info(f"Applying category '{torrent.category}' to destination torrent.")
                    if not dry_run:
                        destination_client.set_category(torrent_hash=torrent.hash, category=torrent.category)
                        time.sleep(1)  # Give it time to apply
                except Exception as e:
                    logging.warning(f"Failed to set category: {e}")

            # 2. Apply tracker-based category if available
            if tracker_rules and not dry_run:
                try:
                    from tracker_manager import set_category_based_on_tracker
                    # Updated to pass TorrentClient instance directly
                    set_category_based_on_tracker(destination_client, torrent.hash, tracker_rules, dry_run)
                    time.sleep(1)
                except Exception as e:
                    logging.warning(f"Failed to apply tracker-based category: {e}")

            # 3. Start Torrent (if configured)
            add_paused = config.getboolean('DESTINATION_CLIENT', 'add_torrents_paused', fallback=True)
            start_after_recheck = config.getboolean('DESTINATION_CLIENT', 'start_torrents_after_recheck', fallback=True)

            if add_paused and start_after_recheck:
                try:
                    logging.info("Resuming destination torrent.")
                    if not dry_run:
                        destination_client.resume_torrent(torrent_hash=torrent.hash)
                        time.sleep(2)  # Verify it started
                        # Check status
                        check_torrent = destination_client.get_torrent_info(torrent_hash=torrent.hash)
                        if check_torrent and check_torrent.state in ['uploading', 'stalledUP', 'queuedUP', 'forcedUP']:
                            logging.info(f"Torrent successfully started: {torrent.name}")
                        else:
                            logging.warning(f"Torrent may not have started correctly. State: {check_torrent.state if check_torrent else 'unknown'}")
                except Exception as e:
                    logging.warning(f"Failed to resume torrent: {e}")

            # 4. Delete Source (AFTER everything else is confirmed)
            if not dry_run and not test_run:
                if config.getboolean('SOURCE_CLIENT', 'delete_after_transfer', fallback=True):
                    try:
                        logging.info(f"Deleting torrent and data from source: {torrent.name}")
                        source_client.delete_torrent(torrent_hash=torrent.hash, delete_files=True)
                        time.sleep(1)  # Give it time to process
                    except Exception as e:
                        logging.warning(f"Failed to delete source torrent: {e}")
                else:
                    logging.info("Skipping source torrent deletion as per 'delete_after_transfer' config.")

            return True, "Post-transfer actions completed successfully."

        except Exception as e:
            # Catch any unexpected error during post-transfer
            logging.error(f"An unexpected error occurred during post-transfer actions: {e}", exc_info=True)
            return False, f"Post-transfer actions failed: {e}"

    # This line should not be reachable, but as a fallback:
    return False, "Post-transfer actions failed due to unhandled recheck status."


def _execute_transfer(
    transfer_mode: str,
    files: List[TransferFile],
    torrent: "qbittorrentapi.TorrentDictionary",
    total_size_calc: int,
    config: configparser.ConfigParser,
    ui: BaseUIManager,
    file_tracker: FileTransferTracker,
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    dry_run: bool,
    # --- New Callbacks ---
    log_transfer: Callable,
    _update_transfer_progress: Callable
) -> bool:
    """
    Executes the file transfer for a given list of files using the specified strategy.

    This function orchestrates the actual data movement by delegating to the appropriate
    strategy function (rsync, sftp, etc.) based on `transfer_mode`.

    Args:
        transfer_mode: The transfer strategy to use ('sftp', 'rsync', etc.).
        files: A list of TransferFile objects representing files to transfer.
        torrent: The source torrent object.
        total_size_calc: The pre-calculated total size of the transfer.
        config: The application configuration.
        ui: The UI manager for progress updates.
        file_tracker: Tracks file progress for resumability.
        ssh_connection_pools: Managed SSH connections.
        dry_run: If True, simulate transfer without moving data.
        log_transfer: Callback for logging transfer events.
        _update_transfer_progress: Callback for updating UI progress.

    Returns:
        bool: True if the transfer was successful, False otherwise.
    """
    hash_ = torrent.hash
    sftp_chunk_size = config['SETTINGS'].getint('sftp_chunk_size_kb', 64) * 1024
    try:
        if transfer_mode == 'sftp':
            source_pool = ssh_connection_pools.get(config['SETTINGS']['source_server_section'])
            if not source_pool: raise ValueError("Source SSH pool not found.")
            download_limit_bytes = int(config['SETTINGS'].getfloat('sftp_download_limit_mbps', 0) * 1024 * 1024 / 8)
            max_concurrent_downloads = config['SETTINGS'].getint('max_concurrent_downloads', 4)
            transfer_content_with_queue(
                source_pool, files, hash_, ui, file_tracker,
                max_concurrent_downloads, dry_run,
                download_limit_bytes, sftp_chunk_size
            )
        elif transfer_mode == 'sftp_upload':
            source_pool = ssh_connection_pools.get(config['SETTINGS']['source_server_section'])
            if not source_pool: raise ValueError("Source SSH pool not found.")
            dest_pool = ssh_connection_pools.get('DESTINATION_SERVER')
            if not dest_pool: raise ValueError("Destination SSH pool not found.")
            max_concurrent_downloads = config['SETTINGS'].getint('max_concurrent_downloads', 4)
            max_concurrent_uploads = config['SETTINGS'].getint('max_concurrent_uploads', 4)
            sftp_download_limit_mbps = config['SETTINGS'].getfloat('sftp_download_limit_mbps', 0)
            sftp_upload_limit_mbps = config['SETTINGS'].getfloat('sftp_upload_limit_mbps', 0)
            download_limit_bytes = int(sftp_download_limit_mbps * 1024 * 1024 / 8)
            upload_limit_bytes = int(sftp_upload_limit_mbps * 1024 * 1024 / 8)
            local_cache = config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
            all_files_tuples = [(f.source_path, f.dest_path) for f in files]
            transfer_content_sftp_upload(
                source_pool, dest_pool, all_files_tuples, hash_, ui,
                file_tracker, max_concurrent_downloads, max_concurrent_uploads, total_size_calc, dry_run,
                local_cache, download_limit_bytes, upload_limit_bytes, sftp_chunk_size
            )
        elif transfer_mode == 'rsync':
            # AI-CONTEXT: Rsync Progress Reporting
            # Rsync operates in two phases: 1. Checksumming (calculating delta), 2. Transferring.
            # During Phase 1, 'transferred_bytes' may jump or appear stalled.
            # To prevent UI >100% bugs, we must clamp progress calculation and rely on Smart Heartbeat during stalls.
            rsync_options = shlex.split(config['SETTINGS'].get("rsync_options", "-avhHSP --partial --inplace"))
            sftp_config = config[config['SETTINGS']['source_server_section']]
            # For rsync, we assume 'files' contains a single entry for the root path
            if not files: raise ValueError("No files provided for rsync transfer.")
            source_content_path = files[0].source_path
            dest_content_path = files[0].dest_path
            heartbeat_callback = ui.pet_watchdog
            transfer_content_rsync(
                sftp_config=sftp_config,
                remote_path=source_content_path,
                local_path=dest_content_path,
                torrent_hash=hash_,
                log_transfer=log_transfer,
                _update_transfer_progress=_update_transfer_progress,
                rsync_options=rsync_options,
                file_tracker=file_tracker,
                total_size=total_size_calc,
                dry_run=dry_run,
                heartbeat_callback=heartbeat_callback
            )
        elif transfer_mode == 'rsync_upload':
            heartbeat_callback = ui.pet_watchdog
            source_server_section = config['SETTINGS'].get('source_server_section', 'SOURCE_SERVER')
            source_config = config[source_server_section]
            dest_server_section = 'DESTINATION_SERVER'
            dest_config = config[dest_server_section]
            rsync_options = shlex.split(config['SETTINGS'].get("rsync_options", "-avhHSP --partial --inplace"))
            source_pool = ssh_connection_pools.get(source_server_section)
            if not source_pool: raise ValueError(f"SSH pool '{source_server_section}' not found.")
            is_folder = False
            with source_pool.get_connection() as (_, ssh):
                is_folder = is_remote_dir(ssh, torrent.content_path)
            # For rsync, we assume 'files' contains a single entry for the root path
            if not files: raise ValueError("No files provided for rsync upload.")
            source_content_path = files[0].source_path
            dest_content_path = files[0].dest_path
            transfer_content_rsync_upload(
                source_config=source_config,
                dest_config=dest_config,
                rsync_options=rsync_options,
                source_content_path=source_content_path,
                dest_content_path=dest_content_path,
                torrent_hash=hash_,
                file_tracker=file_tracker,
                total_size=total_size_calc,
                log_transfer=log_transfer,
                _update_transfer_progress=_update_transfer_progress,
                dry_run=dry_run,
                is_folder=is_folder,
                heartbeat_callback=heartbeat_callback
            )

        return True # Transfer success
    except (RemoteTransferError, Exception) as e:
        logging.error(f"Transfer failed for '{torrent.name}': {e}", exc_info=True)
        return False # Transfer failed

def _execute_transfer_placeholder(
    transfer_mode: str,
    files: List[TransferFile],
    torrent: "qbittorrentapi.TorrentDictionary",
    total_size_calc: int,
    config: configparser.ConfigParser,
    ui: BaseUIManager,
    file_tracker: FileTransferTracker,
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    dry_run: bool,
    # --- Callbacks ---
    log_transfer: Callable,
    _update_transfer_progress: Callable
) -> bool:
    """
    This is a temporary placeholder for the recheck logic until the main
    refactor is complete. It mirrors _execute_transfer.
    """
    return _execute_transfer(
        transfer_mode, files, torrent, total_size_calc, config, ui,
        file_tracker, ssh_connection_pools, dry_run,
        log_transfer, _update_transfer_progress
    )


def transfer_torrent(
    torrent: qbittorrentapi.TorrentDictionary,
    total_size: int,
    source_client: Optional[TorrentClient],
    destination_client: Optional[TorrentClient],
    config: configparser.ConfigParser,
    tracker_rules: Dict[str, str],
    ui: BaseUIManager,
    file_tracker: FileTransferTracker,
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    checkpoint: TransferCheckpoint, # Added checkpoint
    args: argparse.Namespace, # Added args
    # --- New Callbacks ---
    log_transfer: Callable,
    _update_transfer_progress: Callable
) -> Tuple[str, str]:
    """Orchestrates the entire transfer process for a single torrent.

    Lifecycle:
    1. Pre-flight checks (path resolution, size comparison).
    2. Strategy selection (SFTP vs Rsync).
    3. Execution (The actual data transfer).
    4. Post-transfer actions (Add to dest, Force Recheck, Categorize, Delete Source).

    Args:
        torrent: The torrent to transfer.
        total_size: Total size of the torrent content.
        source_qbit: Source qBittorrent client.
        destination_qbit: Destination qBittorrent client.
        config: App configuration.
        tracker_rules: Rules for categorizing torrents.
        ui: UI Manager.
        file_tracker: Tracker for file progress.
        ssh_connection_pools: SSH pools.
        checkpoint: Transfer checkpoint manager.
        args: Command line arguments.
        log_transfer: Logging callback.
        _update_transfer_progress: Progress callback.

    Returns:
        Tuple[str, str]: A status code ('success', 'failed', 'skipped') and a message.
    """
    name, hash_ = torrent.name, torrent.hash
    start_time = time.time()
    dry_run = args.dry_run
    test_run = args.test_run
    sftp_chunk_size = config['SETTINGS'].getint('sftp_chunk_size_kb', 64) * 1024
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

    try:
        # --- 1. Pre-Transfer Setup ---
        (
            pre_transfer_status, pre_transfer_msg,
            source_content_path, dest_content_path,
            destination_save_path
        ) = _pre_transfer_setup(torrent, total_size, config, ssh_connection_pools, args, transfer_mode)

        if pre_transfer_status == "failed":
            return "failed", pre_transfer_msg

        # --- 2. Get Strategy and Prepare File List ---
        strategy = get_transfer_strategy(transfer_mode, config, ssh_connection_pools)
        if not dest_content_path: # Should not happen if status is not failed
             return "failed", "Destination content path could not be determined."
        files: List[TransferFile] = strategy.prepare_files(torrent, dest_content_path)

        if not files and not dry_run:
            logging.warning(f"No files found to transfer for '{name}'. Skipping.")
            return "skipped", "No files found to transfer"

        total_size_calc = sum(f.size for f in files)
        file_names_for_ui = [f.source_path for f in files]

        # --- 3. Setup UI & Handle Pre-Transfer Statuses ---
        local_cache_sftp_upload = config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
        transfer_multiplier = 2 if transfer_mode == 'sftp_upload' and local_cache_sftp_upload else 1
        ui.start_torrent_transfer(
            hash_, name, total_size_calc,
            file_names_for_ui,
            transfer_multiplier
        )

        transfer_executed = False
        if dry_run:
            logging.info(f"[DRY RUN] Simulating transfer for '{name}'. Status: {pre_transfer_status}")
            ui.log(f"[dim]Dry Run: {name} (Status: {pre_transfer_status})[/dim]")
            ui.update_torrent_progress(hash_, total_size_calc * transfer_multiplier, transfer_type='download') # Use a default type
        elif pre_transfer_status == "exists_same_size":
            ui.log(f"[green]Content exists for {name}. Skipping transfer.[/green]")
            ui.update_torrent_progress(hash_, total_size_calc * transfer_multiplier, transfer_type='download')
            transfer_executed = False
        elif pre_transfer_status == "exists_different_size":
            if 'rsync' in transfer_mode:
                logging.info(f"Rsync mode: Resuming transfer for mismatched content: {dest_content_path}")
                # Do nothing, rsync will handle the delta
            else:
                # Original logic for SFTP
                ui.log(f"[yellow]Mismatch size for {name}. Deleting destination content...[/yellow]")
                logging.warning(f"Deleting mismatched destination content: {dest_content_path}")
                try:
                    delete_destination_content(dest_content_path, transfer_mode, ssh_connection_pools)
                    ui.log(f"[green]Deleted mismatched content for {name}.[/green]")
                except Exception as e:
                    logging.exception(f"Failed to delete mismatched content for {name}")
                    return "failed", f"Failed to delete mismatched content: {e}"
            # Fall through to execute the transfer below

        # --- 4. Execute Transfer if Needed ---
        if pre_transfer_status in ("not_exists", "exists_different_size") and not dry_run:
            # This replaces the large try/except block
            transfer_success = _execute_transfer(
                transfer_mode, files, torrent, total_size_calc, config,
                ui, file_tracker, ssh_connection_pools, dry_run,
                log_transfer, _update_transfer_progress
            )

            if not transfer_success:
                return "failed", "Transfer failed during execution"

            transfer_executed = True

        # --- 5. Post-Transfer Actions ---
        post_transfer_success, post_transfer_msg = _post_transfer_actions(
            torrent, source_client, destination_client, config, tracker_rules,
            ssh_connection_pools, dest_content_path, destination_save_path,
            transfer_executed, dry_run, test_run,
            file_tracker,
            transfer_mode,
            files,
            ui,
            # --- Add these new arguments ---
            log_transfer, _update_transfer_progress
        )

        if post_transfer_success:
            logging.info(f"SUCCESS: Successfully processed torrent: {name} (Message: {post_transfer_msg})")
            ui.log(f"[bold green]Success: {name}[/bold green]")
            ui.complete_torrent_transfer(hash_, success=True) # <-- MOVED HERE
            return "success", post_transfer_msg
        else:
            logging.error(f"Post-transfer actions FAILED for {name}: {post_transfer_msg}")
            ui.complete_torrent_transfer(hash_, success=False) # <-- MOVED HERE
            return "failed", post_transfer_msg

    except Exception as e:
        log_message = f"An unexpected error occurred while processing {name}: {e}"
        logging.exception(log_message)
        ui.complete_torrent_transfer(hash_, success=False) # <-- ADD THIS LINE
        return "failed", f"Unexpected error: {e}"

def _handle_utility_commands(args: argparse.Namespace, config: configparser.ConfigParser, tracker_rules: Dict[str, str], script_dir: Path, ssh_connection_pools: Dict[str, SSHConnectionPool], checkpoint: TransferCheckpoint, file_tracker: FileTransferTracker) -> bool:
    """Handles command-line arguments that perform a specific action and exit.

    This function checks for the presence of utility flags (e.g., `--list-rules`,
    `--test-permissions`). If one is found, it executes the corresponding
    action and returns True, signaling to the main function that the script
    should exit.

    Args:
        args: The command-line arguments namespace.
        config: The application's configuration.
        tracker_rules: A dictionary of tracker-to-category rules.
        script_dir: The directory of the running script.
        ssh_connection_pools: Dictionary of SSH connection pools.
        checkpoint: The TransferCheckpoint instance.
        file_tracker: The FileTransferTracker instance.

    Returns:
        True if a utility command was handled, False otherwise.
    """
    if not (args.list_rules or args.add_rule or args.delete_rule or args.categorize or args.interactive_categorize or args.test_permissions or args.clear_recheck_failure or args.clear_corruption):
        return False

    logging.info("Executing utility command...")

    if args.categorize:
        try:
            dest_client_section = config['SETTINGS'].get('destination_client_section', 'DESTINATION_QBITTORRENT')
            dest_client_wrapper = get_client(config[dest_client_section], 'qbittorrent', "Destination")
            if dest_client_wrapper and dest_client_wrapper.connect():
                # ToDo: Expand Interface - TorrentClient needs generic get_torrents(filter)
                if hasattr(dest_client_wrapper, 'client') and dest_client_wrapper.client:
                    destination_qbit = dest_client_wrapper.client
                    logging.info("Fetching all completed torrents from destination for categorization...")
                    completed_torrents = [t for t in destination_qbit.torrents_info() if t.progress == 1]
                    if completed_torrents:
                        categorize_torrents(dest_client_wrapper, completed_torrents, tracker_rules)
                    else:
                        logging.info("No completed torrents found on destination to categorize.")
                else:
                    logging.warning("Client does not expose underlying 'client' object. Cannot fetch torrents.")
            else:
                logging.error("Failed to connect to destination client.")
        except Exception as e:
            logging.error(f"Failed to run categorization: {e}", exc_info=True)
        return True

    if args.test_permissions:
        transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
        dest_path = config['DESTINATION_PATHS'].get('destination_path')
        if not dest_path:
            raise ValueError("FATAL: 'destination_path' is not defined in your config file.")
        remote_config = None
        if transfer_mode == 'sftp_upload':
            if 'DESTINATION_SERVER' not in config:
                raise ValueError("FATAL: 'transfer_mode' is 'sftp_upload' but [DESTINATION_SERVER] is not defined in config.")
            remote_config = config['DESTINATION_SERVER']
        test_path_permissions(dest_path, remote_config=remote_config, ssh_connection_pools=ssh_connection_pools)
        return True

    if args.list_rules:
        display_tracker_rules(tracker_rules)
        return True

    if args.add_rule:
        domain, category = args.add_rule
        tracker_rules[domain] = category
        if save_tracker_rules(tracker_rules, script_dir):
            logging.info(f"Successfully added rule: '{domain}' -> '{category}'.")
        return True

    if args.delete_rule:
        domain_to_delete = args.delete_rule
        if domain_to_delete in tracker_rules:
            del tracker_rules[domain_to_delete]
            if save_tracker_rules(tracker_rules, script_dir):
                logging.info(f"Successfully deleted rule for '{domain_to_delete}'.")
        else:
            logging.warning(f"No rule found for domain '{domain_to_delete}'. Nothing to delete.")
        return True

    if args.interactive_categorize:
        try:
            dest_client_section = config['SETTINGS'].get('destination_client_section', 'DESTINATION_QBITTORRENT')
            dest_client_wrapper = get_client(config[dest_client_section], 'qbittorrent', "Destination")
            if dest_client_wrapper and dest_client_wrapper.connect():
                cat_to_scan = args.category or config['SETTINGS'].get('category_to_move') or ''
                run_interactive_categorization(dest_client_wrapper, tracker_rules, script_dir, cat_to_scan, args.no_rules)
            else:
                logging.error("Failed to connect to destination client.")
        except Exception as e:
            logging.error(f"Failed to run interactive categorization: {e}", exc_info=True)
        return True

    if args.clear_recheck_failure:
        hash_to_clear = args.clear_recheck_failure
        if checkpoint.is_recheck_failed(hash_to_clear):
            checkpoint.clear_recheck_failed(hash_to_clear)
            logging.info(f"Successfully removed torrent hash '{hash_to_clear[:10]}...' from the recheck_failed list.")
        else:
            logging.warning(f"Torrent hash '{hash_to_clear[:10]}...' not found in the recheck_failed list.")
        return True

    if args.clear_corruption:
        hash_to_clear = args.clear_corruption

        # Use file_tracker's lock for thread-safety
        with file_tracker._lock:
            keys_to_remove = [
                key for key in file_tracker.state["corruption_hashes"].keys()
                if key.startswith(hash_to_clear)
            ]

            if keys_to_remove:
                for key in keys_to_remove:
                    del file_tracker.state["corruption_hashes"][key]
                file_tracker._save() # Force a save
                logging.info(
                    f"Cleared {len(keys_to_remove)} corruption markers "
                    f"for torrent {hash_to_clear[:10]}..."
                )
            else:
                logging.warning(
                    f"No corruption markers found for torrent {hash_to_clear[:10]}..."
                )

        return True # We're done

    return False # Should not be reached, but as a fallback.


# --- Main Class-Based Refactor ---

class TorrentMover:
    """
    Main application class to orchestrate the torrent moving process.

    This class encapsulates the state and dependencies required for the
    application to run, such as config, clients, and managers.
    """
    def __init__(self, args: argparse.Namespace, config_manager: ConfigManager, script_dir: Path):
        self.args = args
        self.config_manager = config_manager
        self.config = config_manager.config
        self.script_dir = script_dir
        self.tracker_rules: Dict[str, str] = {}
        self.ssh_connection_pools: Dict[str, SSHConnectionPool] = {}
        self.checkpoint: Optional[TransferCheckpoint] = None
        self.file_tracker: Optional[FileTransferTracker] = None
        self.source_client: Optional[TorrentClient] = None
        self.destination_client: Optional[TorrentClient] = None
        self.ui: BaseUIManager = SimpleUIManager() # Default, will be replaced

    def _initialize_ssh_pools(self):
        """Initializes SSH connection pools based on config."""
        pool_wait_timeout_seconds = self.config['SETTINGS'].getint('pool_wait_timeout', 300)
        server_sections = [s for s in self.config.sections() if s.endswith('_SERVER')]
        for section_name in server_sections:
            max_sessions = self.config[section_name].getint('max_concurrent_ssh_sessions', 8)
            self.ssh_connection_pools[section_name] = SSHConnectionPool(
                host=self.config[section_name]['host'],
                port=self.config[section_name].getint('port'),
                username=self.config[section_name]['username'],
                password=self.config[section_name]['password'],
                max_size=max_sessions,
                connect_timeout=Timeouts.SSH_CONNECT,
                pool_wait_timeout=pool_wait_timeout_seconds
            )
            logging.info(f"Initialized SSH connection pool for '{section_name}' with size {max_sessions}.")

    def pre_flight_checks(self) -> Optional[Tuple[List[Tuple['qbittorrentapi.TorrentDictionary', int]], int, int]]:
        """
        Performs all necessary checks and preparations before starting the UI.
        Returns a tuple of (analyzed_torrents, total_transfer_size, total_count) if successful.
        Returns None if any check fails or if there are no torrents to move.
        """
        # 1. Check Configuration
        print("Checking configuration...", end=" ", flush=True)
        try:
            source_section_name = self.config['SETTINGS']['source_client_section']
            dest_section_name = self.config['SETTINGS']['destination_client_section']
            if source_section_name not in self.config:
                raise KeyError(f"Source section '{source_section_name}' missing.")
            if dest_section_name not in self.config:
                raise KeyError(f"Destination section '{dest_section_name}' missing.")
            print("[OK]")
        except Exception as e:
            print("[FAILED]")
            logging.error(f"Configuration check failed: {e}")
            return None

        # 2. Connect to Source Client
        print("Connecting to Source Client...", end=" ", flush=True)
        try:
            self.source_client = get_client(self.config[source_section_name], 'qbittorrent', "Source")
            if self.source_client and self.source_client.connect():
                 print("[OK]")
            else:
                 raise Exception("Failed to connect")
        except Exception as e:
            print("[FAILED]")
            logging.error(f"Failed to connect to Source Client: {e}")
            return None

        # 3. Connect to Destination Client
        print("Connecting to Destination Client...", end=" ", flush=True)
        try:
            self.destination_client = get_client(self.config[dest_section_name], 'qbittorrent', "Destination")
            if self.destination_client and self.destination_client.connect():
                 print("[OK]")
            else:
                 raise Exception("Failed to connect")
        except Exception as e:
            print("[FAILED]")
            logging.error(f"Failed to connect to Destination Client: {e}")
            return None

        # 4. Cleanup & Recovery
        try:
            # Updated to pass TorrentClient instances directly
            if self.destination_client:
                 cleanup_orphaned_cache(self.destination_client)

            if self.source_client and self.destination_client:
                 recovered_torrents = recover_cached_torrents(self.source_client, self.destination_client)
            else:
                 recovered_torrents = []
        except Exception as e:
            logging.error(f"Error during cache cleanup/recovery: {e}")
            recovered_torrents = []

        # 5. Get Eligible Torrents
        try:
            category_to_move = self.config['SETTINGS']['category_to_move']
            size_threshold_gb_str = self.config['SETTINGS'].get('size_threshold_gb')
            size_threshold_gb = None
            if size_threshold_gb_str and size_threshold_gb_str.strip():
                try:
                    size_threshold_gb = float(size_threshold_gb_str)
                except ValueError:
                    logging.error(f"Invalid value for 'size_threshold_gb': '{size_threshold_gb_str}'. It must be a number.")

            eligible_torrents = self.source_client.get_eligible_torrents(category_to_move, size_threshold_gb)
            if recovered_torrents:
                eligible_hashes = {t.hash for t in eligible_torrents}
                for torrent in recovered_torrents:
                    if torrent.hash not in eligible_hashes:
                        eligible_torrents.append(torrent)
        except Exception as e:
            logging.error(f"Error fetching eligible torrents: {e}")
            return None

        # --- Implement Strict Tracker Exclusion ---
        filtered_torrents = []
        for torrent in eligible_torrents:
            try:
                category = None
                if hasattr(self.source_client, 'client'):
                     category = get_category_from_rules(torrent, self.tracker_rules, self.source_client.client)
                if category and category.lower() == 'ignore':
                    logging.info(f"Skipping torrent '{torrent.name}' due to ignore rule.")
                    continue
            except Exception as e:
                logging.warning(f"Error checking tracker rules for '{torrent.name}': {e}")

            filtered_torrents.append(torrent)

        eligible_torrents = filtered_torrents

        total_count = len(eligible_torrents)
        if total_count == 0:
            logging.info("No torrents to move.")
            print(f"\nNo torrents found in category '{category_to_move}'. Exiting.")
            return None

        # 6. Analysis (Fetching Sizes)
        print("Analyzing eligible torrents...", end=" ", flush=True)
        analyzed_torrents: List[Tuple['qbittorrentapi.TorrentDictionary', int]] = []
        total_transfer_size = 0

        try:
            source_server_section = self.config['SETTINGS'].get('source_server_section', 'SOURCE_SERVER')
            source_pool = self.ssh_connection_pools.get(source_server_section)
            if not source_pool:
                 raise ValueError(f"Error: SSH connection pool for server section '{source_server_section}' not found.")

            paths_to_check = []
            for torrent in eligible_torrents:
                if self.checkpoint.is_recheck_failed(torrent.hash):
                    logging.warning(f"Skipping torrent marked with recheck failure: {torrent.name}")
                    continue
                paths_to_check.append(torrent.content_path)

            if not paths_to_check:
                 print("[OK] (No new torrents to analyze)")
            else:
                with source_pool.get_connection() as (sftp, ssh):
                     sizes = batch_get_remote_sizes(ssh, paths_to_check)

                for torrent in eligible_torrents:
                    if torrent.content_path not in paths_to_check:
                        continue

                    size = sizes.get(torrent.content_path)
                    if size is not None and size > 0:
                        analyzed_torrents.append((torrent, size))
                        total_transfer_size += size
                    elif size == 0:
                        logging.warning(f"Skipping zero-byte torrent: {torrent.name}")
                    else:
                        logging.error(f"Failed to calculate size for: {torrent.name}")

            print("[OK]")
        except Exception as e:
            print("[FAILED]")
            logging.exception(f"Analysis failed: {e}")
            return None

        if not analyzed_torrents:
            print("No valid, non-zero size torrents to transfer.")
            return None

        # 7. Destination Health Check
        print("Checking Destination Disk Space...", end=" ", flush=True)
        if not self.args.dry_run:
            if not destination_health_check(self.config, total_transfer_size, self.ssh_connection_pools):
                 print("[FAILED]")
                 return None
        print("[OK]")

        return analyzed_torrents, total_transfer_size, total_count

    def _update_transfer_progress(self, torrent_hash: str, progress: float, transferred_bytes: int, total_size: int):
        """Callback to update torrent progress.

        # AI-GOTCHA: Transfer progress clamping
        # Progress > 100% breaks the UI progress bar and causes visual glitches.
        # AI-INVARIANT: Progress MUST be clamped to 100.0 (or 1.0 depending on scale) before setting.

        DEBUGGING HINTS:
        - If progress > 100%: Check delta calculation logic.
        - If speeds show 0: Verify last_known_bytes is updating.
        - If UI freezes: Check if _lock is held too long.
        """
        if isinstance(self.ui, UIManagerV2):
            with self.ui._lock:
                if torrent_hash in self.ui._torrents:
                    # --- NEW DELTA LOGIC ---
                    # 1. Get the last known bytes from the torrent's state
                    last_known_bytes = self.ui._torrents[torrent_hash].get("bytes_for_delta_calc", 0)

                    # 2. Calculate the delta
                    delta = transferred_bytes - last_known_bytes

                    # 3. Update the UI's global stats with the delta
                    if delta > 0:
                        # FIX #1: Determine transfer type based on transfer mode
                        transfer_mode = self.config['SETTINGS'].get('transfer_mode', 'sftp').lower()
                        
                        if transfer_mode in ['rsync', 'rsync_upload']:
                            # For rsync modes, we need to determine if this is DL or UL phase
                            # rsync_upload has both phases, rsync only has DL
                            if transfer_mode == 'rsync_upload':
                                # Check if we're in download or upload phase based on progress
                                # First half is download, second half is upload
                                if transferred_bytes <= (total_size / 2):
                                    self.ui._stats["transferred_dl_bytes"] += delta
                                else:
                                    self.ui._stats["transferred_ul_bytes"] += delta
                            else:
                                # Pure rsync mode is download only
                                self.ui._stats["transferred_dl_bytes"] += delta
                        elif transfer_mode == 'sftp_upload':
                            # SFTP upload mode - both phases happen
                            local_cache = self.config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
                            if local_cache:
                                # With cache: first half is DL, second half is UL
                                if transferred_bytes <= (total_size / 2):
                                    self.ui._stats["transferred_dl_bytes"] += delta
                                else:
                                    self.ui._stats["transferred_ul_bytes"] += delta
                            else:
                                # Direct stream: count as upload only
                                self.ui._stats["transferred_ul_bytes"] += delta
                        else:
                            # SFTP mode is download only
                            self.ui._stats["transferred_dl_bytes"] += delta
                        
                        self.ui._stats["transferred_bytes"] = self.ui._stats.get("transferred_dl_bytes", 0) + self.ui._stats.get("transferred_ul_bytes", 0)
                        # Update the overall progress bar
                        self.ui.main_progress.update(self.ui.overall_task, advance=delta)

                    # 4. Update the individual torrent's progress bar with the new TOTAL
                    self.ui._torrents[torrent_hash]["transferred"] = transferred_bytes

                    # 5. Store the new "last known bytes" value back in the state
                    self.ui._torrents[torrent_hash]["bytes_for_delta_calc"] = transferred_bytes
                    # --- END NEW DELTA LOGIC ---

    def _handle_transfer_log(self, torrent_hash: str, message: str):
        """
        Routes log messages from transfer processes to the logging system.
        This ensures that [DEBUG] messages are filtered by the root logger's level.
        """
        if message.startswith("[DEBUG]"):
            # Log the message content *without* the [DEBUG] prefix at DEBUG level
            log_content = message[7:].strip()
            # We use the torrent hash for context
            logging.debug(f"({torrent_hash[:10]}...) {log_content}")
        else:
            # Log other messages (like "Starting transfer...") as INFO
            logging.info(f"({torrent_hash[:10]}...) {message}")


    def _transfer_worker(self, torrent: "qbittorrentapi.TorrentDictionary", total_size: int, is_auto_move: bool = False):
        """
        Worker function to be run in the thread pool for a single torrent.
        """
        if not (self.source_client and self.destination_client and self.config and self.tracker_rules is not None and self.ui and self.file_tracker and self.checkpoint and self.args):
             logging.error("TorrentMover class not fully initialized. Skipping worker.")
             return

        self.ui.log(f"Starting transfer worker for: {torrent.name}")

        try:
            # Call the module-level function, passing all dependencies and callbacks
            status, message = transfer_torrent(
                torrent=torrent,
                total_size=total_size,
                source_client=self.source_client,
                destination_client=self.destination_client,
                config=self.config,
                tracker_rules=self.tracker_rules,
                ui=self.ui,
                file_tracker=self.file_tracker,
                ssh_connection_pools=self.ssh_connection_pools,
                checkpoint=self.checkpoint,
                args=self.args,
                # --- Pass the new callbacks from this class instance ---
                log_transfer=self._handle_transfer_log,
                _update_transfer_progress=self._update_transfer_progress
            )

            log_name = torrent.name[:50] + "..." if len(torrent.name) > 53 else torrent.name
            match status:
                case "success":
                    logging.info(f"Success: {torrent.name} - {message}")
                case "failed":
                    logging.error(f"Failed: {torrent.name} - {message}")
                    self.ui.log(f"[bold red]Failed: {log_name}[/bold red] - {message}")
                case "skipped":
                    logging.info(f"Skipped: {torrent.name} - {message}")
                    self.ui.log(f"[dim]Skipped: {log_name} - {message}[/dim]")
                case "dry_run":
                    logging.info(f"Dry Run: {torrent.name} - {message}")
                    self.ui.log(f"[cyan]Dry Run: {log_name} - {message}[/cyan]")
        except Exception as e:
            logging.error(f"An exception was thrown for torrent '{torrent.name}': {e}", exc_info=True)
            self.ui.complete_torrent_transfer(torrent.hash, success=False)


    def run(self, simple_mode: bool, rich_handler: Optional[logging.Handler]) -> None:
        """The main orchestration function for the torrent transfer process.
        This is the refactored version of _run_transfer_operation.
        """
        # Run pre-flight checks
        # Note: source_qbit and destination_qbit are initialized in pre_flight_checks
        if not (self.config and self.checkpoint and self.file_tracker):
             logging.error("FATAL: Class not initialized before run.")
             return

        pre_flight_result = self.pre_flight_checks()
        if not pre_flight_result:
            return

        analyzed_torrents, total_transfer_size, total_count = pre_flight_result
        transfer_mode = self.config['SETTINGS'].get('transfer_mode', 'sftp').lower()

        # --- UI Initialization ---
        watchdog_timeout = self.config_manager.get_watchdog_timeout()
        if simple_mode:
            self.ui = SimpleUIManager(watchdog_timeout=watchdog_timeout)
        else:
            self.ui = UIManagerV2(version=__version__, rich_handler=rich_handler, watchdog_timeout=watchdog_timeout)

        with self.ui as ui:
            # Assign self.ui now that the context is entered
            self.ui = ui

            ui.set_transfer_mode(transfer_mode)
            ui.set_analysis_total(total_count)
            ui.complete_analysis() # Already done in pre-flight

            local_cache_sftp_upload = self.config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
            transfer_multiplier = 2 if transfer_mode == 'sftp_upload' and local_cache_sftp_upload else 1
            ui.set_overall_total(total_transfer_size * transfer_multiplier)

            logging.info("STATE: Starting transfer phase...")
            ui.log(f"Transferring {len(analyzed_torrents)} torrents... [green]Running[/]")
            ui.log("Executing transfers...")
            try:
                if self.args.parallel_jobs > 1:
                    with ThreadPoolExecutor(max_workers=self.args.parallel_jobs, thread_name_prefix='Transfer') as executor:
                        transfer_futures = {
                            executor.submit(
                                self._transfer_worker, t, size, is_auto_move=False # Assuming manual run
                            ): (t, size) for t, size in analyzed_torrents
                        }
                        for future in as_completed(transfer_futures):
                            torrent, size = transfer_futures[future]
                            try:
                                future.result() # Worker function handles its own logging/UI
                            except Exception as e:
                                logging.error(f"An exception was thrown by worker for torrent '{torrent.name}': {e}", exc_info=True)
                                ui.complete_torrent_transfer(torrent.hash, success=False)
                else:
                    for torrent, size in analyzed_torrents:
                        try:
                            self._transfer_worker(torrent, size, is_auto_move=False)
                        except Exception as e:
                            logging.error(f"An exception was thrown for torrent '{torrent.name}': {e}", exc_info=True)
                            ui.complete_torrent_transfer(torrent.hash, success=False)


            except KeyboardInterrupt:
                ui.log("[bold yellow]Process interrupted by user. Shutting down workers...[/]")
                ui.set_final_status("Shutdown requested.")
                if 'executor' in locals():
                    executor.shutdown(wait=False, cancel_futures=True)
                raise

            completed_count = 0
            if simple_mode:
                simple_ui = cast(SimpleUIManager, ui)
                completed_count = simple_ui._stats['completed_transfers']
            else:
                rich_ui = cast(UIManagerV2, ui)
                with rich_ui._lock:
                    completed_count = rich_ui._stats['completed_transfers']

            ui.log(f"Processing complete. Moved {completed_count}/{total_count} torrent(s).")
            ui.set_final_status("All tasks finished.")
            logging.info(f"Processing complete. Successfully moved {completed_count}/{total_count} torrent(s).")


def main() -> int:
    """The main entry point for the application.

    This function is responsible for:
    -   Acquiring a lock to prevent multiple instances from running.
    -   Parsing command-line arguments.
    -   Performing startup checks (e.g., screen detection, version check).
    -   Setting up file and console logging.
    -   Loading and validating the configuration.
    -   Initializing SSH connection pools.
    -   Handling utility commands or dispatching to the main transfer operation.
    -   Ensuring all resources (like SSH pools) are cleaned up on exit.

    Returns:
        0 on successful execution, 1 on error.
    """
    script_dir = Path(__file__).resolve().parent
    lock = None
    try:
        lock = LockFile(script_dir / 'torrent_mover.lock')
        lock.acquire()
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    default_config_path = script_dir / 'config.ini'
    parser = argparse.ArgumentParser(description="A script to move qBittorrent torrents and data between servers.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--config', default=str(default_config_path), help='Path to the configuration file.')
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--dry-run', action='store_true', help='Simulate the process without making any changes.')
    mode_group.add_argument('--test-run', action='store_true', help='Run the full process but do not delete the source torrent.')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging to file.')
    parser.add_argument('--simple', action='store_true', help='Use a simple, non-interactive UI. Recommended for `screen` or `tmux`.')
    parser.add_argument('--parallel-jobs', type=int, default=DEFAULT_PARALLEL_JOBS, metavar='N', help='Number of torrents to process in parallel.')
    parser.add_argument('-l', '--list-rules', action='store_true', help='List all tracker-to-category rules and exit.')
    parser.add_argument('-a', '--add-rule', nargs=2, metavar=('TRACKER_DOMAIN', 'CATEGORY'), help='Add or update a rule and exit.')
    parser.add_argument('-d', '--delete-rule', metavar='TRACKER_DOMAIN', help='Delete a rule and exit.')
    parser.add_argument('-c', '--categorize', dest='categorize', action='store_true', help='Categorize all completed torrents on destination based on tracker rules.')
    parser.add_argument('-i', '--interactive-categorize', dest='interactive_categorize', action='store_true', help='Interactively categorize torrents on destination.')
    parser.add_argument('--category', help='(For -i mode) Specify a category to scan, overriding the config.')
    parser.add_argument('-nr', '--no-rules', action='store_true', help='(For -i mode) Ignore existing rules and show all torrents in the category.')
    parser.add_argument('--test-permissions', action='store_true', help='Test write permissions for the configured destination_path and exit.')
    parser.add_argument('--check-config', action='store_true', help='Validate the configuration file and exit.')
    parser.add_argument('--version', action='store_true', help="Show program's version and config file path, then exit.")
    parser.add_argument('--clear-recheck-failure', metavar='TORRENT_HASH', help='Remove a torrent hash from the recheck_failed list in the checkpoint file.')
    parser.add_argument(
        '--clear-corruption',
        metavar='TORRENT_HASH',
        help='Clear corruption markers for a specific torrent to allow retry'
    )
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    # --- Screen Detection ---
    term_env = os.getenv('TERM', '').lower()
    if 'screen' in term_env and not args.simple:
        print("--- WARNING: 'screen' DETECTED ---", file=sys.stderr)
        print("You are running in a 'screen' session, which is known to cause", file=sys.stderr)
        print("severe UI corruption with the rich interactive display.", file=sys.stderr)
        print("\nIt is HIGHLY recommended to stop and re-run with the --simple flag:", file=sys.stderr)
        print(f"  python3 -m torrent_mover.torrent_mover --simple { ' '.join(sys.argv[1:]) }", file=sys.stderr)
        print("\nAlternatively, run in a standard terminal (not 'screen') to use the rich UI.", file=sys.stderr)
        print("\nAborting in 10 seconds. Press Ctrl+C to cancel.", file=sys.stderr)
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            print("\nUser cancelled. Exiting.", file=sys.stderr)
            return 1
        print("Aborting. Please re-run with --simple.", file=sys.stderr)
        return 1
    # --- End Screen Detection ---

    if args.version:
        print(f"{Path(sys.argv[0]).name} {__version__}")
        print(f"Configuration file: {args.config}")
        return 0
    setup_logging(script_dir, args.dry_run, args.test_run, args.debug)

    logger = logging.getLogger()
    log_level = logging.DEBUG if args.debug else logging.INFO
    rich_handler: Optional[logging.Handler] = None
    simple_mode = args.simple

    if simple_mode:
        # --- Simple Mode: Add a standard StreamHandler ---
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)
        logging.info("--- Torrent Mover script started (Simple UI) ---")
    else:
        # --- Rich Mode: Add the RichHandler ---
        rich_handler = RichHandler(level=log_level, show_path=False, rich_tracebacks=True, markup=True, console=Console(stderr=True))
        rich_formatter = logging.Formatter('%(message)s')
        rich_handler.setFormatter(rich_formatter)
        logging.info("--- Torrent Mover script started (Rich UI) ---")

    logging.info(f"Using configuration file: {args.config}")
    if args.check_config:
        logging.info("--- Running Configuration Check ---")
        try:
            ConfigManager(args.config)
            logging.info("[bold green]SUCCESS:[/] Configuration file appears to be valid.")
            return 0
        except SystemExit:
            logging.error("[bold red]FAILURE:[/] Configuration file has errors.")
            return 1

    config_template_path = script_dir / 'config.ini.template'
    update_config(args.config, str(config_template_path))

    checkpoint = TransferCheckpoint(script_dir / 'transfer_checkpoint.json')
    file_tracker = FileTransferTracker(script_dir / 'file_transfer_tracker.json')

    # Initialize config and mover first
    config_manager = ConfigManager(args.config)
    mover = TorrentMover(args, config_manager, script_dir)

    try:
        mover._initialize_ssh_pools()
        mover.tracker_rules = load_tracker_rules(script_dir)
        mover.checkpoint = checkpoint
        mover.file_tracker = file_tracker

        if _handle_utility_commands(args, mover.config, mover.tracker_rules, script_dir, mover.ssh_connection_pools, checkpoint, file_tracker):
            return 0

        transfer_mode = mover.config['SETTINGS'].get('transfer_mode', 'sftp').lower()
        if transfer_mode == 'rsync' or transfer_mode == 'rsync_upload':
            check_sshpass_installed()

        # Clients are now connected within mover.run() -> mover.pre_flight_checks()
        mover.run(simple_mode, rich_handler)

    except KeyboardInterrupt:
        logging.warning("Process interrupted by user. Shutting down.")
    except KeyError as e:
        logging.error(f"Configuration key missing: {e}. Please check your config.ini.")
        return 1
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)
        return 1
    finally:
        for pool in mover.ssh_connection_pools.values():
            pool.close_all()
        logging.info("All SSH connections have been closed.")
        if lock and lock._acquired:
            lock.release()
            logging.debug("Script lock released.")
        logging.info("--- Torrent Mover script finished ---")
    return 0

if __name__ == "__main__":
    sys.exit(main())
