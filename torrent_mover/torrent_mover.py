#!/usr/bin/env python3
# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

__version__ = "2.9.5" # Note: Version updated in user's file, retaining it.

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
from .config_manager import update_config, load_config, ConfigValidator
from .ssh_manager import (
    SSHConnectionPool, check_sshpass_installed,
    _get_all_files_recursive, batch_get_remote_sizes,
    is_remote_dir
)
from .qbittorrent_manager import (
    connect_qbit, get_eligible_torrents, wait_for_recheck_completion,
    get_incomplete_files
)
from .transfer_manager import (
    FileTransferTracker, TransferCheckpoint, transfer_content_rsync,
    transfer_content_with_queue, transfer_content_sftp_upload, Timeouts,
    transfer_content_rsync_upload, RemoteTransferError
)
from .system_manager import (
    LockFile, setup_logging, destination_health_check, change_ownership,
    test_path_permissions, cleanup_orphaned_cache,
    recover_cached_torrents, delete_destination_content,
    delete_destination_files
)
from .tracker_manager import (
    categorize_torrents,
    load_tracker_rules, save_tracker_rules, set_category_based_on_tracker,
    run_interactive_categorization, display_tracker_rules
)
from .transfer_strategies import get_transfer_strategy, TransferFile
from .ui import BaseUIManager, SimpleUIManager, UIManagerV2
from .watchdog import TransferWatchdog
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
    """Orchestrates the entire transfer process for a single torrent."""
    name, hash_ = torrent.name, torrent.hash
    dry_run = args.dry_run
    test_run = args.test_run
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    strategy = get_transfer_strategy(transfer_mode, config, ssh_connection_pools)

    try:
        # 1. Pre-transfer check
        (
            pre_transfer_status, pre_transfer_msg,
            source_content_path, dest_content_path,
            destination_save_path
        ) = strategy.pre_transfer_check(torrent, total_size, args)

        if pre_transfer_status in ("failed", "skipped"):
            return pre_transfer_status, pre_transfer_msg
        if not dest_content_path:
             return "failed", "Destination content path could not be determined."

        # 2. Prepare files for transfer
        files: List[TransferFile] = strategy.prepare_files(torrent, dest_content_path)
        if not files and not dry_run:
            return "skipped", "No files to transfer."

        # 3. Initialize UI for this torrent
        total_size_calc = sum(f.size for f in files)
        file_names_for_ui = [f.source_path for f in files]
        local_cache_sftp_upload = config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
        transfer_multiplier = 2 if transfer_mode == 'sftp_upload' and local_cache_sftp_upload else 1

        ui.start_torrent_transfer(hash_, name, total_size_calc, file_names_for_ui, transfer_multiplier)

        # 4. Execute transfer if required
        transfer_executed = False
        if dry_run:
            ui.update_torrent_progress(hash_, total_size_calc * transfer_multiplier, transfer_type='download')
            ui.log(f"[cyan]Dry Run: '{name}' processing complete.[/cyan]")

        elif pre_transfer_status == "exists_same_size":
            ui.update_torrent_progress(hash_, total_size_calc * transfer_multiplier, transfer_type='download')
            ui.log(f"Skipping transfer for '{name}', content exists and is the same size.")
            transfer_executed = False # Content already exists

        elif pre_transfer_status in ("not_exists", "exists_different_size"):
            # Delete mismatched content if it exists and we're not using rsync
            if pre_transfer_status == "exists_different_size" and 'rsync' not in transfer_mode:
                try:
                    delete_destination_content(dest_content_path, transfer_mode, ssh_connection_pools)
                except Exception as e:
                    return "failed", f"Failed to delete mismatched content: {e}"

            transfer_success = strategy.execute_transfer(files, torrent, ui, file_tracker, dry_run)
            if not transfer_success:
                return "failed", f"File transfer failed for '{name}'."
            transfer_executed = True

        # 5. Perform post-transfer actions (add to client, set category, etc.)
        post_transfer_success, post_transfer_msg = strategy.post_transfer_actions(
            torrent, source_qbit, destination_qbit, config, tracker_rules,
            dest_content_path, destination_save_path,
            transfer_executed, dry_run, test_run, file_tracker, files, ui
        )

        if post_transfer_success:
            ui.complete_torrent_transfer(hash_, success=True)
            return "success", post_transfer_msg
        else:
            ui.complete_torrent_transfer(hash_, success=False)
            return "failed", post_transfer_msg

    except Exception as e:
        logging.error(f"An unexpected error occurred during transfer_torrent for '{name}': {e}", exc_info=True)
        ui.complete_torrent_transfer(hash_, success=False)
        return "failed", f"Unexpected error in '{name}': {e}"

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
            destination_qbit = connect_qbit(config[dest_client_section], "Destination")
            logging.info("Fetching all completed torrents from destination for categorization...")
            completed_torrents = [t for t in destination_qbit.torrents_info() if t.progress == 1]
            if completed_torrents:
                categorize_torrents(destination_qbit, completed_torrents, tracker_rules)
            else:
                logging.info("No completed torrents found on destination to categorize.")
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
    def __init__(self, args: argparse.Namespace, config: configparser.ConfigParser, script_dir: Path):
        self.args = args
        self.config = config
        self.script_dir = script_dir
        self.tracker_rules: Dict[str, str] = {}
        self.ssh_connection_pools: Dict[str, SSHConnectionPool] = {}
        self.checkpoint: Optional[TransferCheckpoint] = None
        self.file_tracker: Optional[FileTransferTracker] = None
        self.source_qbit: Optional["qbittorrentapi.Client"] = None
        self.destination_qbit: Optional["qbittorrentapi.Client"] = None
        self.ui: BaseUIManager = SimpleUIManager() # Default, will be replaced
        self.watchdog: Optional[TransferWatchdog] = None

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

    def _connect_qbit_clients(self):
        """Connects to source and destination qBittorrent clients."""
        try:
            source_section_name = self.config['SETTINGS']['source_client_section']
            dest_section_name = self.config['SETTINGS']['destination_client_section']
            self.source_qbit = connect_qbit(self.config[source_section_name], "Source")
            self.destination_qbit = connect_qbit(self.config[dest_section_name], "Destination")
        except KeyError as e:
            raise KeyError(f"Configuration Error: The client section '{e}' is defined in your [SETTINGS] but not found in the config file.") from e
        except Exception as e:
            raise RuntimeError(f"Failed to connect to qBittorrent client after multiple retries: {e}") from e

    def _transfer_worker(self, torrent: "qbittorrentapi.TorrentDictionary", total_size: int, is_auto_move: bool = False):
        """
        Worker function to be run in the thread pool for a single torrent.
        """
        if not (self.source_qbit and self.destination_qbit and self.config and self.tracker_rules and self.ui and self.file_tracker and self.checkpoint and self.args):
             logging.error("TorrentMover class not fully initialized. Skipping worker.")
             return

        self.ui.log(f"Starting transfer worker for: {torrent.name}")

        try:
            # Call the module-level function, passing all dependencies and callbacks
            status, message = transfer_torrent(
                torrent=torrent,
                total_size=total_size,
                source_qbit=self.source_qbit,
                destination_qbit=self.destination_qbit,
                config=self.config,
                tracker_rules=self.tracker_rules,
                ui=self.ui,
                file_tracker=self.file_tracker,
                ssh_connection_pools=self.ssh_connection_pools,
                checkpoint=self.checkpoint,
                args=self.args
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
        if not (self.source_qbit and self.destination_qbit and self.config and self.checkpoint and self.file_tracker):
            logging.error("FATAL: Class not initialized before run.")
            return

        cleanup_orphaned_cache(self.destination_qbit)
        recovered_torrents = recover_cached_torrents(self.source_qbit, self.destination_qbit)
        category_to_move = self.config['SETTINGS']['category_to_move']
        size_threshold_gb_str = self.config['SETTINGS'].get('size_threshold_gb')
        size_threshold_gb = None
        if size_threshold_gb_str and size_threshold_gb_str.strip():
            try:
                size_threshold_gb = float(size_threshold_gb_str)
            except ValueError:
                logging.error(f"Invalid value for 'size_threshold_gb': '{size_threshold_gb_str}'. It must be a number. Disabling threshold.")

        eligible_torrents = get_eligible_torrents(self.source_qbit, category_to_move, size_threshold_gb)
        if recovered_torrents:
            eligible_hashes = {t.hash for t in eligible_torrents}
            for torrent in recovered_torrents:
                if torrent.hash not in eligible_hashes:
                    eligible_torrents.append(torrent)

        if not eligible_torrents:
            logging.info("No torrents to move.")
            return

        total_count = len(eligible_torrents)
        transfer_mode = self.config['SETTINGS'].get('transfer_mode', 'sftp').lower()

        # --- UI Initialization ---
        if simple_mode:
            self.ui = SimpleUIManager()
        else:
            self.ui = UIManagerV2(version=__version__, rich_handler=rich_handler)

        with self.ui as ui:
            # Assign self.ui now that the context is entered
            self.ui = ui

            # Configure the watchdog
            watchdog_timeout = self.config['SETTINGS'].getint('watchdog_timeout_seconds', 600)
            self.watchdog = TransferWatchdog(timeout_seconds=watchdog_timeout)
            try:
                self.watchdog.start(ui)
                ui.set_transfer_mode(transfer_mode)
                ui.set_analysis_total(total_count)
                ui.log(f"Found {total_count} torrents to process. Analyzing...")

                analyzed_torrents: List[Tuple['qbittorrentapi.TorrentDictionary', int]] = []
                total_transfer_size = 0
                logging.info("STATE: Starting analysis phase...")
                try:
                    source_server_section = self.config['SETTINGS'].get('source_server_section', 'SOURCE_SERVER')
                    source_pool = self.ssh_connection_pools.get(source_server_section)
                    if not source_pool:
                        raise ValueError(f"Error: SSH connection pool for server section '{source_server_section}' not found.")

                    # Unified logic: Get all paths first
                    paths_to_check = []
                    for torrent in eligible_torrents:
                        if self.checkpoint.is_recheck_failed(torrent.hash):
                            logging.warning(f"Skipping torrent marked with recheck failure: {torrent.name}")
                            ui.log(f"[yellow]Skipped (recheck fail): {torrent.name}[/yellow]")
                            ui.advance_analysis()
                            continue
                        paths_to_check.append(torrent.content_path)

                    if not paths_to_check:
                        logging.info("No new torrents to analyze.")
                    else:
                        # Get all sizes in one go
                        with source_pool.get_connection() as (sftp, ssh):
                            logging.debug(f"Batch analyzing size for {len(paths_to_check)} torrents...")
                            sizes = batch_get_remote_sizes(ssh, paths_to_check)

                        # Process the results
                        for torrent in eligible_torrents:
                            if torrent.content_path not in paths_to_check:
                                continue # Was skipped due to recheck failure

                            try:
                                size = sizes.get(torrent.content_path)
                                if size is not None and size > 0:
                                    analyzed_torrents.append((torrent, size))
                                    total_transfer_size += size
                                    logging.debug(f"Analyzed '{torrent.name}': {size} bytes")
                                elif size == 0:
                                    logging.warning(f"Skipping zero-byte torrent: {torrent.name}")
                                else: # size is None
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

                local_cache_sftp_upload = self.config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
                transfer_multiplier = 2 if transfer_mode == 'sftp_upload' and local_cache_sftp_upload else 1
                ui.set_overall_total(total_transfer_size * transfer_multiplier)

                if not self.args.dry_run and not destination_health_check(self.config, total_transfer_size, self.ssh_connection_pools):
                    ui.log("[bold red]Destination health check failed. Aborting transfer process.[/]")
                    logging.error("FATAL: Destination health check failed.")
                    time.sleep(5)
                    raise RuntimeError("Destination health check failed.")

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
            finally:
                if self.watchdog:
                    self.watchdog.stop()


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

    # Initialize config and mover first
    config = load_config(args.config)
    mover = TorrentMover(args, config, script_dir)

    try:
        validator = ConfigValidator(config)
        if not validator.validate():
            return 1

        mover._initialize_ssh_pools()
        mover.tracker_rules = load_tracker_rules(script_dir)
        mover.checkpoint = checkpoint
        mover.file_tracker = file_tracker

        if _handle_utility_commands(args, config, mover.tracker_rules, script_dir, mover.ssh_connection_pools, checkpoint, file_tracker):
            return 0

        transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
        if transfer_mode == 'rsync' or transfer_mode == 'rsync_upload':
            check_sshpass_installed()

        # Connect clients before running
        mover._connect_qbit_clients()

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
