#!/usr/bin/env python3
# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

__version__ = "2.2.0"

# Standard Lib
import configparser
import sys
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import time
import argparse
import argcomplete
from typing import Dict, List, Tuple, TYPE_CHECKING

# Project Modules
from .config_manager import update_config, load_config, ConfigValidator
from .ssh_manager import (
    SSHConnectionPool, check_sshpass_installed,
    _get_all_files_recursive, get_remote_size_rsync, batch_get_remote_sizes
)
from .qbittorrent_manager import connect_qbit, get_eligible_torrents, wait_for_recheck_completion
from .transfer_manager import (
    FileTransferTracker, TransferCheckpoint, transfer_content_rsync,
    transfer_content, transfer_content_sftp_upload, Timeouts
)
from .system_manager import (
    LockFile, setup_logging, destination_health_check, change_ownership,
    _delete_destination_content, test_path_permissions, cleanup_orphaned_cache,
    recover_cached_torrents
)
from .tracker_manager import (
    load_tracker_rules, save_tracker_rules, set_category_based_on_tracker,
    run_interactive_categorization, display_tracker_rules
)
from .ui import UIManagerV2 as UIManager

if TYPE_CHECKING:
    import qbittorrentapi

# --- Constants ---
DEFAULT_PARALLEL_JOBS = 4

def _pre_transfer_setup(torrent: 'qbittorrentapi.TorrentDictionary', config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> Tuple[List[Tuple[str, str]], str, str, str, int]:
    """Handles pre-transfer setup including path resolution and file listing."""
    source_pool = ssh_connection_pools.get('SOURCE_SERVER')
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    source_content_path = torrent.content_path.rstrip('/\\')
    dest_base_path = config['DESTINATION_PATHS']['destination_path']
    content_name = os.path.basename(source_content_path)
    dest_content_path = os.path.join(dest_base_path, content_name)
    remote_dest_base_path = config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
    destination_save_path = remote_dest_base_path
    all_files: List[Tuple[str, str]] = []
    if transfer_mode != 'rsync':
        if not source_pool:
            raise ValueError("Source SSH connection pool not initialized.")
        with source_pool.get_connection() as (sftp, ssh):
            source_stat = sftp.stat(source_content_path)
            if source_stat.st_mode & 0o40000:  # S_ISDIR
                _get_all_files_recursive(sftp, source_content_path, dest_content_path, all_files)
            else:
                all_files.append((source_content_path, dest_content_path))
    else:
        all_files.append((source_content_path, dest_content_path))
    total_files = len(all_files)
    return all_files, source_content_path, dest_content_path, destination_save_path, total_files

def _execute_transfer(torrent: 'qbittorrentapi.TorrentDictionary', total_size: int, config: configparser.ConfigParser, ui: UIManager, file_tracker: FileTransferTracker, ssh_connection_pools: Dict[str, SSHConnectionPool], all_files: List[Tuple[str, str]], source_content_path: str, dest_content_path: str, dry_run: bool, sftp_chunk_size: int) -> None:
    """Executes the file transfer based on the configured transfer mode."""
    name = torrent.name
    hash_ = torrent.hash
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
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

def _post_transfer_actions(torrent: 'qbittorrentapi.TorrentDictionary', source_qbit: 'qbittorrentapi.Client', destination_qbit: 'qbittorrentapi.Client', config: configparser.ConfigParser, tracker_rules: Dict[str, str], ssh_connection_pools: Dict[str, SSHConnectionPool], dest_content_path: str, destination_save_path: str, dry_run: bool, test_run: bool, checkpoint: TransferCheckpoint, ui: UIManager) -> bool:
    """Handles all actions after a successful transfer."""
    name, hash_ = torrent.name, torrent.hash
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    chown_user = config['SETTINGS'].get('chown_user', '').strip()
    chown_group = config['SETTINGS'].get('chown_group', '').strip()
    if chown_user or chown_group:
        remote_config = config['DESTINATION_SERVER'] if transfer_mode == 'sftp_upload' else None
        change_ownership(dest_content_path, chown_user, chown_group, remote_config, dry_run, ssh_connection_pools)
    destination_save_path_str = str(destination_save_path).replace("\\", "/")
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
        logging.info(f"[DRY RUN] Would export and add torrent to Destination (paused) with save path '{destination_save_path_str}': {name}")
    if not dry_run:
        logging.info(f"CLIENT: Triggering force recheck on Destination for: {name}")
        destination_qbit.torrents_recheck(torrent_hashes=hash_)
    else:
        logging.info(f"[DRY RUN] Would trigger force recheck on Destination for: {name}")
    if not wait_for_recheck_completion(destination_qbit, hash_, dry_run=dry_run):
        logging.error(f"Failed to verify recheck for {name}. Assuming destination data is corrupt.")
        ui.log(f"[bold red]Recheck FAILED: {name}. Deleting destination data.[/bold red]")
        if not dry_run:
            logging.warning(f"Deleting destination data to force re-transfer on next run: {dest_content_path}")
            _delete_destination_content(dest_content_path, config, ssh_connection_pools)
        else:
            logging.info(f"[DRY RUN] Would delete destination data: {dest_content_path}")
        checkpoint.mark_recheck_failed(hash_)
        logging.warning(f"Marked torrent {hash_} as recheck_failed in checkpoint.")
        return False
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
    else:
        logging.info(f"[DRY RUN/TEST RUN] Would pause torrent on Source: {name}")
    if test_run:
        logging.info(f"[TEST RUN] Skipping deletion of torrent from Source: {name}")
    elif not dry_run:
        logging.info(f"CLIENT: Deleting torrent and data from Source: {name}")
        source_qbit.torrents_delete(torrent_hashes=hash_, delete_files=True)
    else:
        logging.info(f"[DRY RUN] Would delete torrent and data from Source: {name}")
    return True

def transfer_torrent(torrent: 'qbittorrentapi.TorrentDictionary', total_size: int, source_qbit: 'qbittorrentapi.Client', destination_qbit: 'qbittorrentapi.Client', config: configparser.ConfigParser, tracker_rules: Dict[str, str], ui: UIManager, file_tracker: FileTransferTracker, ssh_connection_pools: Dict[str, SSHConnectionPool], checkpoint: TransferCheckpoint, dry_run: bool = False, test_run: bool = False, sftp_chunk_size: int = 65536) -> Tuple[bool, float]:
    """
    Executes the transfer and management process for a single, pre-analyzed torrent.
    """
    name, hash_ = torrent.name, torrent.hash
    success = False
    start_time = time.time()

    local_cache_sftp_upload = config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    transfer_multiplier = 2 if transfer_mode == 'sftp_upload' and local_cache_sftp_upload else 1

    try:
        all_files, source_content_path, dest_content_path, destination_save_path, total_files = _pre_transfer_setup(
            torrent, config, ssh_connection_pools
        )

        ui.start_torrent_transfer(hash_, name, total_size, total_files, transfer_multiplier)

        if dry_run:
            logging.info(f"[DRY RUN] Simulating transfer for '{name}'.")
            ui.update_torrent_progress(hash_, total_size * transfer_multiplier)
            success = True
            duration = time.time() - start_time
            return True, duration

        _execute_transfer(
            torrent, total_size, config, ui, file_tracker, ssh_connection_pools,
            all_files, source_content_path, dest_content_path, dry_run, sftp_chunk_size
        )

        if _post_transfer_actions(
            torrent, source_qbit, destination_qbit, config, tracker_rules,
            ssh_connection_pools, dest_content_path, destination_save_path, dry_run, test_run,
            checkpoint, ui
        ):
            logging.info(f"SUCCESS: Successfully processed torrent: {name}")
            ui.log(f"[bold green]Success: {name}[/bold green]")
            success = True
            duration = time.time() - start_time
            return True, duration
        else:
            duration = time.time() - start_time
            return False, duration

    except Exception as e:
        logging.error(f"An error occurred while processing torrent {name}: {e}", exc_info=True)
        success = False
        duration = time.time() - start_time
        return False, duration
    finally:
        ui.complete_torrent_transfer(hash_, success=success)

def _handle_utility_commands(args: argparse.Namespace, config: configparser.ConfigParser, tracker_rules: Dict[str, str], script_dir: Path, ssh_connection_pools: Dict[str, SSHConnectionPool], checkpoint: TransferCheckpoint) -> bool:
    """Handles all utility command-line arguments that exit after running."""
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

    return False # Should not be reached, but as a fallback.

def _run_transfer_operation(config: configparser.ConfigParser, args: argparse.Namespace, tracker_rules: Dict[str, str], script_dir: Path, ssh_connection_pools: Dict[str, SSHConnectionPool], checkpoint: TransferCheckpoint, file_tracker: FileTransferTracker) -> None:
    """Connects to clients and runs the main transfer process."""
    sftp_chunk_size = config['SETTINGS'].getint('sftp_chunk_size_kb', 64) * 1024
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

    if checkpoint.state["completed"]:
        original_count = len(eligible_torrents)
        eligible_torrents = [t for t in eligible_torrents if not checkpoint.is_completed(t.hash)]
        skipped_count = original_count - len(eligible_torrents)
        if skipped_count > 0:
            logging.info(f"Skipped {skipped_count} torrent(s) that were already completed in a previous run.")

    if "recheck_failed" in checkpoint.state and checkpoint.state["recheck_failed"]:
        original_count = len(eligible_torrents)
        failed_hashes_to_skip = {h for h in checkpoint.state["recheck_failed"]}  # Use a set for faster lookups
        torrents_before_filtering = eligible_torrents
        eligible_torrents = [t for t in eligible_torrents if t.hash not in failed_hashes_to_skip]
        skipped_failed_count = original_count - len(eligible_torrents)
        if skipped_failed_count > 0:
            logging.warning(f"Skipped {skipped_failed_count} torrent(s) marked as 'recheck_failed' from previous runs. Manual intervention may be required.")
            skipped_names = [t.name for t in torrents_before_filtering if t.hash in failed_hashes_to_skip]
            if skipped_names:
                logging.warning(f"Skipped failed torrents: {', '.join(skipped_names)}")

    if not eligible_torrents:
        logging.info("No torrents to move after filtering completed and failed torrents.")
        return

    total_count = len(eligible_torrents)
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

    with UIManager(version=__version__) as ui:
        ui.set_transfer_mode(transfer_mode)
        ui.set_analysis_total(total_count)
        ui.log(f"Found {total_count} torrents to process. Analyzing...")
        sftp_config = config['SOURCE_SERVER']
        analyzed_torrents: List[Tuple['qbittorrentapi.TorrentDictionary', int]] = []
        total_transfer_size = 0
        logging.info("STATE: Starting analysis phase...")
        try:
            if transfer_mode == 'rsync':
                for torrent in eligible_torrents:
                    try:
                        size = get_remote_size_rsync(sftp_config, torrent.content_path)
                        if size > 0:
                            analyzed_torrents.append((torrent, size))
                            total_transfer_size += size
                        else:
                            pass
                    except Exception as e:
                        ui.log(f"[bold red]Error calculating size for '{torrent.name}': {e}[/]")
                    finally:
                        ui.advance_analysis()
            else:
                source_server_section = config['SETTINGS'].get('source_server_section', 'SOURCE_SERVER')
                source_pool = ssh_connection_pools.get(source_server_section)
                if not source_pool:
                    raise ValueError(f"Error: SSH connection pool for server section '{source_server_section}' not found. Check config.")
                with source_pool.get_connection() as (sftp, ssh):
                    paths = [t.content_path for t in eligible_torrents]
                    sizes = batch_get_remote_sizes(ssh, paths)
                    for torrent in eligible_torrents:
                        size = sizes.get(torrent.content_path)
                        if size is not None and size > 0:
                            analyzed_torrents.append((torrent, size))
                            total_transfer_size += size
                        elif size == 0:
                            pass
                        else:
                            pass
                        ui.advance_analysis()
        except Exception as e:
            raise RuntimeError(f"A critical error occurred during the analysis phase: {e}") from e

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
            with ThreadPoolExecutor(max_workers=args.parallel_jobs, thread_name_prefix='Transfer') as executor:
                transfer_futures = {
                    executor.submit(
                        transfer_torrent, t, size, source_qbit, destination_qbit, config, tracker_rules,
                        ui, file_tracker, ssh_connection_pools, checkpoint, args.dry_run, args.test_run, sftp_chunk_size
                    ): (t, size) for t, size in analyzed_torrents
                }
                for future in as_completed(transfer_futures):
                    torrent, size = transfer_futures[future]
                    try:
                        success, duration = future.result()
                        if success:
                            checkpoint.mark_completed(torrent.hash)
                    except Exception as e:
                        logging.error(f"An exception was thrown for torrent '{torrent.name}': {e}", exc_info=True)
                        ui.complete_torrent_transfer(torrent.hash, success=False)
        except KeyboardInterrupt:
            ui.log("[bold yellow]Process interrupted by user. Transfers cancelled.[/]")
            ui.set_final_status("Shutdown requested.")
            raise

        with ui._lock:
            completed_count = ui._stats['completed_transfers']
        ui.log(f"Processing complete. Moved {completed_count}/{total_count} torrent(s).")
        ui.set_final_status("All tasks finished.")
        with ui._lock:
            completed_count = ui._stats['completed_transfers']
        logging.info(f"Processing complete. Successfully moved {completed_count}/{total_count} torrent(s).")


def main() -> int:
    """Main entry point for the script."""
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
    if args.version:
        print(f"{Path(sys.argv[0]).name} {__version__}")
        print(f"Configuration file: {args.config}")
        return 0
    setup_logging(script_dir, args.dry_run, args.test_run, args.debug)
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

        _run_transfer_operation(config, args, tracker_rules, script_dir, ssh_connection_pools, checkpoint, file_tracker)

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
