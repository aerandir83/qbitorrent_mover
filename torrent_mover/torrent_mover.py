#!/usr/bin/env python3
"""Main entry point for the Torrent Mover application.

This script orchestrates the entire process of moving torrents. It handles
command-line argument parsing, configuration loading, connecting to clients
and servers, and managing the main transfer loop. It delegates specific tasks
to the various manager modules.
"""
# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

__version__ = "2.7.3"

# Standard Lib
import configparser
import sys
import logging
import traceback
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
import argparse
import argcomplete
from typing import Dict, List, Tuple, TYPE_CHECKING, Optional, cast
import typing

import qbittorrentapi
from rich.logging import RichHandler

# Project Modules
from .config_manager import update_config, load_config, ConfigValidator
from .ssh_manager import (
    SSHConnectionPool, check_sshpass_installed,
    _get_all_files_recursive, batch_get_remote_sizes,
    is_remote_dir
)
from .qbittorrent_manager import connect_qbit, get_eligible_torrents, wait_for_recheck_completion
from .transfer_manager import (
    FileTransferTracker, TransferCheckpoint, transfer_content_rsync,
    transfer_content, transfer_content_sftp_upload, Timeouts
)
from .system_manager import (
    LockFile, setup_logging, destination_health_check, change_ownership,
    test_path_permissions, cleanup_orphaned_cache,
    recover_cached_torrents, delete_destination_content
)
from .tracker_manager import (
    load_tracker_rules, save_tracker_rules, set_category_based_on_tracker,
    run_interactive_categorization, display_tracker_rules
)
from .ui import BaseUIManager, SimpleUIManager, UIManagerV2
from rich.logging import RichHandler
from rich.console import Console

if TYPE_CHECKING:
    import qbittorrentapi

# --- Constants ---
DEFAULT_PARALLEL_JOBS = 4

def _pre_transfer_setup(
    torrent: qbittorrentapi.TorrentDictionary,
    total_size: int,
    config: configparser.ConfigParser,
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    args: argparse.Namespace
) -> Tuple[str, str, Optional[List[Tuple[str, str]]], Optional[str], Optional[str], Optional[str], Optional[int]]:
    """Performs pre-flight checks and setup for a torrent transfer.

    This function resolves source and destination paths, checks if the destination
    content already exists, and compares its size with the source. It also
    gathers the list of files to be transferred if necessary.

    Args:
        torrent: The qBittorrent torrent object.
        total_size: The pre-calculated total size of the torrent content.
        config: The application's configuration.
        ssh_connection_pools: A dictionary of SSH connection pools.
        args: The parsed command-line arguments.

    Returns:
        A tuple containing:
        - status_code (str): "not_exists", "exists_same_size",
          "exists_different_size", or "failed".
        - message (str): A human-readable message describing the status.
        - all_files (list|None): A list of (source_path, dest_path) tuples.
        - source_content_path (str|None): The resolved source content path.
        - dest_content_path (str|None): The resolved destination content path.
        - destination_save_path (str|None): The save path for the torrent client.
        - total_files (int|None): The total number of files in the torrent.
    """
    source_pool = ssh_connection_pools.get(config['SETTINGS']['source_server_section'])
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    source_content_path = torrent.content_path.rstrip('/\\')
    dest_base_path = config['DESTINATION_PATHS']['destination_path']
    content_name = os.path.basename(source_content_path)
    dest_content_path = os.path.join(dest_base_path, content_name)
    remote_dest_base_path = config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
    destination_save_path = remote_dest_base_path
    all_files: List[Tuple[str, str]] = []
    is_remote_dest = (transfer_mode == 'sftp_upload')

    destination_exists = False
    destination_size = -1

    if args.dry_run:
        logging.info(f"[DRY RUN] Would check for existing content at: {dest_content_path}")
    else:
        try:
            if is_remote_dest:
                dest_server_section = config['SETTINGS'].get('destination_server_section', 'DESTINATION_SERVER')
                dest_pool = ssh_connection_pools.get(dest_server_section)
                if not dest_pool:
                    return "failed", f"SSH pool '{dest_server_section}' not found.", None, None, None, None, None

                logging.debug(f"Checking remote destination size for: {dest_content_path}")
                with dest_pool.get_connection() as (sftp, ssh):
                    size_map = batch_get_remote_sizes(ssh, [dest_content_path])
                    if dest_content_path in size_map:
                        destination_size = size_map[dest_content_path]
                        destination_exists = True
            else:
                logging.debug(f"Checking local destination size for: {dest_content_path}")
                if os.path.exists(dest_content_path):
                    destination_exists = True
                    if os.path.isdir(dest_content_path):
                        destination_size = sum(f.stat().st_size for f in Path(dest_content_path).glob('**/*') if f.is_file())
                    else:
                        destination_size = os.path.getsize(dest_content_path)

        except Exception as e:
            logging.exception(f"Failed to check destination path '{dest_content_path}'")
            return "failed", f"Failed to check destination path: {e}", None, None, None, None, None

    status_code = "not_exists"
    status_message = "Destination path is clear."

    if destination_exists:
        if total_size == destination_size:
            status_code = "exists_same_size"
            status_message = f"Destination content exists and size matches source ({total_size} bytes). Skipping transfer."
            logging.warning(status_message)
        else:
            status_code = "exists_different_size"
            status_message = f"Destination content exists but size mismatches (Source: {total_size} vs Dest: {destination_size}). Will delete and re-transfer."
            logging.warning(status_message)
    else:
        logging.debug(f"Destination path does not exist: {dest_content_path}")

    if status_code in ["not_exists", "exists_different_size"] and transfer_mode != 'rsync':
        if not source_pool:
            return "failed", "Source SSH connection pool not initialized.", None, None, None, None, None
        try:
            logging.debug(f"Getting file list for: {source_content_path}")
            with source_pool.get_connection() as (sftp, ssh):
                source_stat = sftp.stat(source_content_path)
                if source_stat.st_mode & 0o40000:
                    _get_all_files_recursive(sftp, source_content_path, dest_content_path, all_files)
                else:
                    all_files.append((source_content_path, dest_content_path))
        except Exception as e:
            logging.exception(f"Failed to get file list from source path '{source_content_path}'")
            return "failed", f"Failed to get source file list: {e}", None, None, None, None, None
    elif transfer_mode == 'rsync':
        all_files.append((source_content_path, dest_content_path))

    total_files = len(all_files) if all_files else 0

    return status_code, status_message, all_files, source_content_path, dest_content_path, destination_save_path, total_files

def _execute_transfer(torrent: 'qbittorrentapi.TorrentDictionary', total_size: int, config: configparser.ConfigParser, ui: BaseUIManager, file_tracker: FileTransferTracker, ssh_connection_pools: Dict[str, SSHConnectionPool], all_files: List[Tuple[str, str]], source_content_path: str, dest_content_path: str, dry_run: bool, sftp_chunk_size: int) -> Tuple[bool, str]:
    """Executes the file transfer based on the configured mode.

    Args:
        torrent: The torrent object being transferred.
        total_size: The total size of the torrent's content.
        config: The application's configuration.
        ui: The UI manager instance.
        file_tracker: The file transfer tracker instance.
        ssh_connection_pools: A dictionary of SSH connection pools.
        all_files: A list of (source, destination) path tuples.
        source_content_path: The top-level source path.
        dest_content_path: The top-level destination path.
        dry_run: A boolean indicating if this is a dry run.
        sftp_chunk_size: The chunk size for SFTP transfers.

    Returns:
        A tuple (success, message).
    """
    name = torrent.name
    hash_ = torrent.hash
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    try:
        if transfer_mode == 'rsync':
            transfer_content_rsync(config['SOURCE_SERVER'], source_content_path, dest_content_path, hash_, ui, dry_run)
            logging.info(f"TRANSFER: Rsync transfer completed for '{name}'.")
        else:
            max_concurrent_downloads = config['SETTINGS'].getint('max_concurrent_downloads', 4)
            max_concurrent_uploads = config['SETTINGS'].getint('max_concurrent_uploads', 4)
            sftp_download_limit_mbps = config['SETTINGS'].getfloat('sftp_download_limit_mbps', 0)
            sftp_upload_limit_mbps = config['SETTINGS'].getfloat('sftp_upload_limit_mbps', 0)
            download_limit_bytes = int(sftp_download_limit_mbps * 1024 * 1024 / 8)
            upload_limit_bytes = int(sftp_upload_limit_mbps * 1024 * 1024 / 8)
            source_pool = ssh_connection_pools.get('SOURCE_SERVER')
            if not source_pool:
                raise ValueError("Source SSH connection pool not initialized.")
            if transfer_mode == 'sftp_upload':
                logging.info(f"TRANSFER: Starting SFTP-to-SFTP upload for '{name}'...")
                dest_pool = ssh_connection_pools.get('DESTINATION_SERVER')
                if not dest_pool:
                    raise ValueError("Destination SSH connection pool not initialized for sftp_upload mode.")
                local_cache_sftp_upload = config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
                transfer_content_sftp_upload(
                    source_pool, dest_pool, all_files, hash_, ui,
                    file_tracker, max_concurrent_downloads, max_concurrent_uploads, total_size, dry_run,
                    local_cache_sftp_upload,
                    download_limit_bytes, upload_limit_bytes, sftp_chunk_size
                )
                logging.info(f"TRANSFER: SFTP-to-SFTP upload completed for '{name}'.")
            else:
                logging.info(f"TRANSFER: Starting SFTP download for '{name}'...")
                transfer_content(source_pool, all_files, hash_, ui, file_tracker, max_concurrent_downloads, dry_run, download_limit_bytes, sftp_chunk_size)
                logging.info(f"TRANSFER: SFTP download completed for '{name}'.")
        return True, "Transfer successful."
    except Exception:
        logging.error(f"Transfer function failed for '{name}'. See above logs for details.")
        return False, "Transfer function failed."

def _post_transfer_actions(
    torrent: qbittorrentapi.TorrentDictionary,
    source_qbit: qbittorrentapi.Client,
    destination_qbit: qbittorrentapi.Client,
    config: configparser.ConfigParser,
    tracker_rules: Dict[str, str],
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    dest_content_path: str,
    destination_save_path: str,
    transfer_executed: bool,
    dry_run: bool = False,
    test_run: bool = False
) -> Tuple[bool, str]:
    """Handles all client and server actions after a transfer is complete.

    This includes changing file ownership, adding the torrent to the destination
    client, triggering a recheck, handling recheck failure, and deleting the
    torrent from the source client.

    Args:
        torrent: The torrent object.
        source_qbit: The source qBittorrent client.
        destination_qbit: The destination qBittorrent client.
        config: The application's configuration.
        tracker_rules: The tracker categorization rules.
        ssh_connection_pools: A dictionary of SSH connection pools.
        dest_content_path: The final path of the torrent content on the destination.
        destination_save_path: The save path for the destination client.
        transfer_executed: A boolean indicating if files were actually moved.
        dry_run: A boolean indicating if this is a dry run.
        test_run: A boolean indicating if this is a test run.

    Returns:
        A tuple (success, message).
    """
    name, hash_ = torrent.name, torrent.hash
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

    if transfer_executed and not dry_run:
        chown_user = config['SETTINGS'].get('chown_user', '').strip()
        chown_group = config['SETTINGS'].get('chown_group', '').strip()
        if chown_user or chown_group:
            remote_config = config['DESTINATION_SERVER'] if transfer_mode == 'sftp_upload' else None
            change_ownership(dest_content_path, chown_user, chown_group, remote_config, dry_run, ssh_connection_pools)

    destination_save_path_str = str(destination_save_path).replace("\\", "/")

    try:
        dest_torrent = destination_qbit.torrents_info(torrent_hashes=hash_)
        if not dest_torrent:
            logging.info(f"Torrent {name} not found on destination. Adding it.")
            if not dry_run:
                logging.debug(f"Exporting .torrent file for {name}")
                torrent_file_content = source_qbit.torrents_export(torrent_hash=hash_)
                logging.info(f"CLIENT: Adding torrent to Destination (paused) with save path '{destination_save_path_str}': {name}")
                destination_qbit.torrents_add(
                    torrent_files=torrent_file_content,
                    save_path=destination_save_path_str,
                    is_paused=True,
                    category=torrent.category,
                    use_auto_torrent_management=True
                )
                time.sleep(5)
            else:
                logging.info(f"[DRY RUN] Would add torrent to Destination (paused) with save path '{destination_save_path_str}': {name}")
        else:
            logging.info(f"Torrent {name} already exists on destination. Proceeding to recheck.")

    except Exception as e:
        logging.exception(f"Failed to add torrent {name} to destination")
        return False, f"Failed to add torrent to destination: {e}"

    if not dry_run:
        logging.info(f"CLIENT: Triggering force recheck on Destination for: {name}")
        try:
            destination_qbit.torrents_recheck(torrent_hashes=hash_)
        except qbittorrentapi.exceptions.NotFound404Error:
            msg = f"Failed to start recheck: Torrent {name} disappeared from destination client."
            logging.error(msg)
            return False, msg
    else:
        logging.info(f"[DRY RUN] Would trigger force recheck on Destination for: {name}")

    if not wait_for_recheck_completion(destination_qbit, hash_, dry_run=dry_run):
        msg = f"Recheck FAILED for {name}. Destination data is corrupt or incomplete."
        logging.error(msg)
        if not transfer_executed:
            logging.warning(f"Deleting corrupt destination data for {name} to force re-transfer on next run.")
            if not dry_run:
                try:
                    delete_destination_content(dest_content_path, transfer_mode, ssh_connection_pools)
                except Exception as e:
                    msg = f"Recheck failed, AND failed to delete corrupt data: {e}"
                    logging.exception(msg)
                    return False, msg
            else:
                logging.info(f"[DRY RUN] Would delete corrupt destination data at: {dest_content_path}")
        return False, msg

    if not dry_run:
        logging.info(f"CLIENT: Starting torrent on Destination: {name}")
        destination_qbit.torrents_resume(torrent_hashes=hash_)
    else:
        logging.info(f"[DRY RUN] Would start torrent on Destination: {name}")

    logging.info(f"CLIENT: Attempting to categorize torrent on Destination: {name}")
    set_category_based_on_tracker(destination_qbit, hash_, tracker_rules, dry_run=dry_run)

    if not dry_run and not test_run:
        logging.info(f"CLIENT: Pausing torrent on Source before deletion: {name}")
        source_qbit.torrents_pause(torrent_hashes=hash_)
        logging.info(f"CLIENT: Deleting torrent and data from Source: {name}")
        source_qbit.torrents_delete(torrent_hashes=hash_, delete_files=True)
    elif test_run:
        logging.info(f"[TEST RUN] Skipping deletion of torrent from Source: {name}")
    else:
        logging.info(f"[DRY RUN] Would pause and delete torrent and data from Source: {name}")

    return True, "Successfully transferred and verified."

def transfer_torrent(
    torrent: qbittorrentapi.TorrentDictionary,
    total_size: int,
    source_qbit: qbittorrentapi.Client,
    destination_qbit: qbittorrentapi.Client,
    config: configparser.ConfigParser,
    tracker_rules: Dict[str, str],
    ui: BaseUIManager,
    file_tracker: FileTransferTracker,
    ssh_connection_pools: Dict[str, SSHConnectionPool],
    checkpoint: TransferCheckpoint,
    args: argparse.Namespace
) -> Tuple[str, str]:
    """Orchestrates the entire transfer process for a single torrent.

    This function coordinates the pre-transfer setup, the transfer execution,
    and the post-transfer client actions for one torrent.

    Args:
        torrent: The torrent object to process.
        total_size: The total size of the torrent content.
        source_qbit: The source qBittorrent client.
        destination_qbit: The destination qBittorrent client.
        config: The application's configuration.
        tracker_rules: The tracker categorization rules.
        ui: The UI manager instance.
        file_tracker: The file transfer tracker instance.
        ssh_connection_pools: A dictionary of SSH connection pools.
        checkpoint: The transfer checkpoint instance.
        args: Parsed command-line arguments.

    Returns:
        A tuple (status, message), where status is one of "success", "failed",
        "skipped", or "dry_run".
    """
    name, hash_ = torrent.name, torrent.hash
    dry_run = args.dry_run
    test_run = args.test_run
    sftp_chunk_size = config['SETTINGS'].getint('sftp_chunk_size_kb', 64) * 1024
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

    try:
        (
            pre_transfer_status, pre_transfer_msg, all_files,
            source_content_path, dest_content_path,
            destination_save_path, total_files
        ) = _pre_transfer_setup(torrent, total_size, config, ssh_connection_pools, args)

        if pre_transfer_status == "failed":
            return "failed", pre_transfer_msg

        if not all_files and not dry_run:
            if transfer_mode == 'rsync':
                 logging.debug("Rsync mode, file list not pre-populated.")
            else:
                logging.warning(f"No files found to transfer for '{name}'. Skipping.")
                return "skipped", "No files found to transfer"

        local_cache_sftp_upload = config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
        transfer_multiplier = 2 if transfer_mode == 'sftp_upload' and local_cache_sftp_upload else 1
        file_names_for_ui: List[str] = []
        if all_files:
            file_names_for_ui = [source_path for source_path, dest_path in all_files]
        elif total_files > 0:
            file_names_for_ui = [f"file {i+1}" for i in range(total_files)]
        ui.start_torrent_transfer(
            hash_, name, total_size,
            file_names_for_ui,
            transfer_multiplier
        )

        transfer_executed = False

        if dry_run:
            logging.info(f"[DRY RUN] Simulating transfer for '{name}'. Status: {pre_transfer_status}")
            ui.log(f"[dim]Dry Run: {name} (Status: {pre_transfer_status})[/dim]")
            ui.update_torrent_progress(hash_, total_size * transfer_multiplier)

        elif pre_transfer_status == "exists_different_size":
            ui.log(f"[yellow]Mismatch size for {name}. Deleting destination content...[/yellow]")
            logging.warning(f"Deleting mismatched destination content: {dest_content_path}")
            try:
                delete_destination_content(dest_content_path, transfer_mode, ssh_connection_pools)
                ui.log(f"[green]Deleted mismatched content for {name}.[/green]")
            except Exception as e:
                logging.exception(f"Failed to delete mismatched content for {name}")
                return "failed", f"Failed to delete mismatched content: {e}"

            transfer_executed, _ = _execute_transfer(
                torrent, total_size, config, ui, file_tracker, ssh_connection_pools,
                all_files, source_content_path, dest_content_path, dry_run, sftp_chunk_size
            )

        elif pre_transfer_status == "not_exists":
            transfer_executed, _ = _execute_transfer(
                torrent, total_size, config, ui, file_tracker, ssh_connection_pools,
                all_files, source_content_path, dest_content_path, dry_run, sftp_chunk_size
            )

        elif pre_transfer_status == "exists_same_size":
            ui.log(f"[green]Content exists for {name}. Skipping transfer.[/green]")
            ui.update_torrent_progress(hash_, total_size * transfer_multiplier)
            transfer_executed = False

        post_transfer_success, post_transfer_msg = _post_transfer_actions(
            torrent, source_qbit, destination_qbit, config, tracker_rules,
            ssh_connection_pools, dest_content_path, destination_save_path,
            transfer_executed,
            dry_run, test_run
        )

        if post_transfer_success:
            logging.info(f"SUCCESS: Successfully processed torrent: {name} (Message: {post_transfer_msg})")
            ui.log(f"[bold green]Success: {name}[/bold green]")
            return "success", post_transfer_msg
        else:
            logging.error(f"Post-transfer actions FAILED for {name}: {post_transfer_msg}")
            return "failed", post_transfer_msg

    except Exception as e:
        log_message = f"An unexpected error occurred while processing {name}: {e}"
        logging.exception(log_message)
        return "failed", f"Unexpected error: {e}"
    finally:
        ui.complete_torrent_transfer(hash_, success=True)

def _handle_utility_commands(args: argparse.Namespace, config: configparser.ConfigParser, tracker_rules: Dict[str, str], script_dir: Path, ssh_connection_pools: Dict[str, SSHConnectionPool], checkpoint: TransferCheckpoint) -> bool:
    """Handles utility command-line arguments that exit after execution.

    Args:
        args: The parsed command-line arguments.
        config: The application's configuration.
        tracker_rules: The tracker categorization rules.
        script_dir: The script's base directory.
        ssh_connection_pools: A dictionary of SSH connection pools.
        checkpoint: The transfer checkpoint instance.

    Returns:
        True if a utility command was handled, False otherwise.
    """
    if not (args.list_rules or args.add_rule or args.delete_rule or args.interactive_categorize or args.test_permissions or args.clear_recheck_failure):
        return False

    logging.info("Executing utility command...")

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
            destination_qbit = connect_qbit(config[dest_client_section], "Destination")
            cat_to_scan = args.category or config['SETTINGS'].get('category_to_move') or ''
            run_interactive_categorization(destination_qbit, tracker_rules, script_dir, cat_to_scan, args.no_rules)
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

    return False

def _run_transfer_operation(config: configparser.ConfigParser, args: argparse.Namespace, tracker_rules: Dict[str, str], script_dir: Path, ssh_connection_pools: Dict[str, SSHConnectionPool], checkpoint: TransferCheckpoint, file_tracker: FileTransferTracker, simple_mode: bool, rich_handler: Optional[logging.Handler]) -> None:
    """Runs the main transfer process after initialization.

    This function connects to the clients, finds eligible torrents, analyzes them,
    performs a health check, and then executes the transfers in a parallel loop.

    Args:
        config: The application's configuration.
        args: The parsed command-line arguments.
        tracker_rules: The tracker categorization rules.
        script_dir: The script's base directory.
        ssh_connection_pools: A dictionary of SSH connection pools.
        checkpoint: The transfer checkpoint instance.
        file_tracker: The file transfer tracker instance.
        simple_mode: A boolean indicating if the simple UI should be used.
        rich_handler: A reference to the rich logging handler.
    """
    try:
        source_section_name = config['SETTINGS']['source_client_section']
        dest_section_name = config['SETTINGS']['destination_client_section']
        source_qbit = connect_qbit(config[source_section_name], "Source")
        destination_qbit = connect_qbit(config[dest_section_name], "Destination")
    except KeyError as e:
        raise KeyError(f"Configuration Error: The client section '{e}' is defined in your [SETTINGS] but not found in the config file.") from e
    except Exception as e:
        raise RuntimeError(f"Failed to connect to qBittorrent client after multiple retries: {e}") from e

    cleanup_orphaned_cache(destination_qbit)
    recovered_torrents = recover_cached_torrents(source_qbit, destination_qbit)
    category_to_move = config['SETTINGS']['category_to_move']
    size_threshold_gb_str = config['SETTINGS'].get('size_threshold_gb')
    size_threshold_gb = None
    if size_threshold_gb_str and size_threshold_gb_str.strip():
        try:
            size_threshold_gb = float(size_threshold_gb_str)
        except ValueError:
            logging.error(f"Invalid value for 'size_threshold_gb': '{size_threshold_gb_str}'. It must be a number. Disabling threshold.")

    eligible_torrents = get_eligible_torrents(source_qbit, category_to_move, size_threshold_gb)
    if recovered_torrents:
        eligible_hashes = {t.hash for t in eligible_torrents}
        for torrent in recovered_torrents:
            if torrent.hash not in eligible_hashes:
                eligible_torrents.append(torrent)

    if not eligible_torrents:
        logging.info("No torrents to move.")
        return

    total_count = len(eligible_torrents)
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

    ui_context: BaseUIManager
    if simple_mode:
        ui_context = SimpleUIManager()
    else:
        ui_context = UIManagerV2(version=__version__, rich_handler=rich_handler)

    with ui_context as ui:
        ui.set_transfer_mode(transfer_mode)
        ui.set_analysis_total(total_count)
        ui.log(f"Found {total_count} torrents to process. Analyzing...")
        analyzed_torrents: List[Tuple['qbittorrentapi.TorrentDictionary', int]] = []
        total_transfer_size = 0
        logging.info("STATE: Starting analysis phase...")
        try:
            source_server_section = config['SETTINGS'].get('source_server_section', 'SOURCE_SERVER')
            source_pool = ssh_connection_pools.get(source_server_section)
            if not source_pool:
                raise ValueError(f"Error: SSH connection pool for server section '{source_server_section}' not found.")

            paths_to_check = []
            for torrent in eligible_torrents:
                if checkpoint.is_recheck_failed(torrent.hash):
                    logging.warning(f"Skipping torrent marked with recheck failure: {torrent.name}")
                    ui.log(f"[yellow]Skipped (recheck fail): {torrent.name}[/yellow]")
                    ui.advance_analysis()
                    continue
                paths_to_check.append(torrent.content_path)

            if not paths_to_check:
                logging.info("No new torrents to analyze.")
            else:
                with source_pool.get_connection() as (sftp, ssh):
                    logging.debug(f"Batch analyzing size for {len(paths_to_check)} torrents...")
                    sizes = batch_get_remote_sizes(ssh, paths_to_check)

                for torrent in eligible_torrents:
                    if torrent.content_path not in paths_to_check:
                        continue

                    try:
                        size = sizes.get(torrent.content_path)
                        if size is not None and size > 0:
                            analyzed_torrents.append((torrent, size))
                            total_transfer_size += size
                            logging.debug(f"Analyzed '{torrent.name}': {size} bytes")
                        elif size == 0:
                            logging.warning(f"Skipping zero-byte torrent: {torrent.name}")
                        else:
                            logging.error(f"Failed to calculate size for: {torrent.name}. It may not exist on source.")
                    except Exception as e:
                        logging.exception(f"Error during torrent analysis for '{torrent.name}'")
                        ui.log(f"[bold red]Error analyzing {torrent.name}: {e}[/bold red]")
                    finally:
                        ui.advance_analysis()

        except Exception as e:
            logging.exception(f"A critical error occurred during the analysis phase: {e}")
            ui.set_final_status(f"Analysis failed: {e}")
            time.sleep(5)
            raise

        logging.info("STATE: Analysis complete.")
        ui.complete_analysis()
        ui.log("Analysis complete. Verifying destination...")

        if not analyzed_torrents:
            ui.set_final_status("No valid, non-zero size torrents to transfer.")
            logging.info("No valid, non-zero size torrents to transfer.")
            time.sleep(2)
            return

        local_cache_sftp_upload = config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
        transfer_multiplier = 2 if transfer_mode == 'sftp_upload' and local_cache_sftp_upload else 1
        ui.set_overall_total(total_transfer_size * transfer_multiplier)

        if not args.dry_run and not destination_health_check(config, total_transfer_size, ssh_connection_pools):
            ui.log("[bold red]Destination health check failed. Aborting transfer process.[/]")
            logging.error("FATAL: Destination health check failed.")
            time.sleep(5)
            raise RuntimeError("Destination health check failed.")

        logging.info("STATE: Starting transfer phase...")
        ui.log(f"Transferring {len(analyzed_torrents)} torrents... [green]Running[/]")
        ui.log("Executing transfers...")
        try:
            if args.parallel_jobs > 1:
                with ThreadPoolExecutor(max_workers=args.parallel_jobs, thread_name_prefix='Transfer') as executor:
                    transfer_futures = {
                        executor.submit(
                            transfer_torrent, t, size, source_qbit, destination_qbit, config, tracker_rules,
                            ui, file_tracker, ssh_connection_pools, checkpoint, args
                        ): (t, size) for t, size in analyzed_torrents
                    }
                    for future in as_completed(transfer_futures):
                        torrent, size = transfer_futures[future]
                        try:
                            status, message = future.result()
                            log_name = torrent.name[:50] + "..." if len(torrent.name) > 53 else torrent.name
                            match status:
                                case "success":
                                    logging.info(f"Success: {torrent.name} - {message}")
                                case "failed":
                                    logging.error(f"Failed: {torrent.name} - {message}")
                                    ui.log(f"[bold red]Failed: {log_name}[/bold red] - {message}")
                                case "skipped":
                                    logging.info(f"Skipped: {torrent.name} - {message}")
                                    ui.log(f"[dim]Skipped: {log_name} - {message}[/dim]")
                                case "dry_run":
                                    logging.info(f"Dry Run: {torrent.name} - {message}")
                                    ui.log(f"[cyan]Dry Run: {log_name} - {message}[/cyan]")
                        except Exception as e:
                            logging.error(f"An exception was thrown for torrent '{torrent.name}': {e}", exc_info=True)
                            ui.complete_torrent_transfer(torrent.hash, success=False)
            else:
                for torrent, size in analyzed_torrents:
                    status, message = transfer_torrent(
                        torrent, size, source_qbit, destination_qbit, config, tracker_rules,
                        ui, file_tracker, ssh_connection_pools, checkpoint, args
                    )
                    log_name = torrent.name[:50] + "..." if len(torrent.name) > 53 else torrent.name
                    match status:
                        case "success":
                            logging.info(f"Success: {torrent.name} - {message}")
                        case "failed":
                            logging.error(f"Failed: {torrent.name} - {message}")
                            ui.log(f"[bold red]Failed: {log_name}[/bold red] - {message}")
                        case "skipped":
                            logging.info(f"Skipped: {torrent.name} - {message}")
                            ui.log(f"[dim]Skipped: {log_name} - {message}[/dim]")
                        case "dry_run":
                            logging.info(f"Dry Run: {torrent.name} - {message}")
                            ui.log(f"[cyan]Dry Run: {log_name} - {message}[/cyan]")

        except KeyboardInterrupt:
            ui.log("[bold yellow]Process interrupted by user. Transfers cancelled.[/]")
            ui.set_final_status("Shutdown requested.")
            raise

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
    """Main entry point for the script.

    Parses command-line arguments, sets up logging, initializes all managers
    and connection pools, and runs either a utility command or the main
    transfer operation.

    Returns:
        0 on success, 1 on failure.
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
    parser.add_argument('-c', '--categorize', dest='interactive_categorize', action='store_true', help='Interactively categorize torrents on destination.')
    parser.add_argument('--category', help='(For -c mode) Specify a category to scan, overriding the config.')
    parser.add_argument('-nr', '--no-rules', action='store_true', help='(For -c mode) Ignore existing rules and show all torrents in the category.')
    parser.add_argument('--test-permissions', action='store_true', help='Test write permissions for the configured destination_path and exit.')
    parser.add_argument('--check-config', action='store_true', help='Validate the configuration file and exit.')
    parser.add_argument('--version', action='store_true', help="Show program's version and config file path, then exit.")
    parser.add_argument('--clear-recheck-failure', metavar='TORRENT_HASH', help='Remove a torrent hash from the recheck_failed list in the checkpoint file.')
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

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
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)
        logging.info("--- Torrent Mover script started (Simple UI) ---")
    else:
        rich_handler = RichHandler(level=log_level, show_path=False, rich_tracebacks=True, markup=True, console=Console(stderr=True))
        rich_formatter = logging.Formatter('%(message)s')
        rich_handler.setFormatter(rich_formatter)
        logger.addHandler(rich_handler)
        logging.info("--- Torrent Mover script started (Rich UI) ---")

    logging.info(f"Using configuration file: {args.config}")
    if args.check_config:
        logging.info("--- Running Configuration Check ---")
        config = load_config(args.config)
        validator = ConfigValidator(config)
        if validator.validate():
            logging.info("[bold green]SUCCESS:[/] Configuration file appears to be valid.")
            return 0
        else:
            logging.error("[bold red]FAILURE:[/] Configuration file has errors.")
            return 1
    config_template_path = script_dir / 'config.ini.template'
    update_config(args.config, str(config_template_path))
    checkpoint = TransferCheckpoint(script_dir / 'transfer_checkpoint.json')
    file_tracker = FileTransferTracker(script_dir / 'file_transfer_tracker.json')
    ssh_connection_pools: Dict[str, SSHConnectionPool] = {}
    try:
        config = load_config(args.config)
        validator = ConfigValidator(config)
        if not validator.validate():
            return 1
        server_sections = [s for s in config.sections() if s.endswith('_SERVER')]
        for section_name in server_sections:
            max_sessions = config[section_name].getint('max_concurrent_ssh_sessions', 8)
            ssh_connection_pools[section_name] = SSHConnectionPool(
                host=config[section_name]['host'],
                port=config[section_name].getint('port'),
                username=config[section_name]['username'],
                password=config[section_name]['password'],
                max_size=max_sessions,
                connect_timeout=Timeouts.SSH_CONNECT,
                pool_wait_timeout=Timeouts.POOL_WAIT
            )
            logging.info(f"Initialized SSH connection pool for '{section_name}' with size {max_sessions}.")

        tracker_rules = load_tracker_rules(script_dir)
        if _handle_utility_commands(args, config, tracker_rules, script_dir, ssh_connection_pools, checkpoint):
            return 0

        transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
        if transfer_mode == 'rsync':
            check_sshpass_installed()

        _run_transfer_operation(config, args, tracker_rules, script_dir, ssh_connection_pools, checkpoint, file_tracker, simple_mode, rich_handler)

    except KeyboardInterrupt:
        logging.warning("Process interrupted by user. Shutting down.")
    except KeyError as e:
        logging.error(f"Configuration key missing: {e}. Please check your config.ini.")
        return 1
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)
        return 1
    finally:
        for pool in ssh_connection_pools.values():
            pool.close_all()
        logging.info("All SSH connections have been closed.")
        logging.info("--- Torrent Mover script finished ---")
    return 0

if __name__ == "__main__":
    sys.exit(main())
