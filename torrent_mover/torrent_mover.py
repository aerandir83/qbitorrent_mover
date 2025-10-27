#!/usr-bin/env python3
# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

__version__ = "2.0.0"

import configparser
import sys
import logging
from pathlib import Path
import paramiko
import json
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import subprocess
import re
import shlex
import threading
from collections import defaultdict
import errno
from .config_manager import update_config, load_config, ConfigValidator
from .utils import retry, LockFile
from .qbittorrent_manager import connect_qbit, get_eligible_torrents, wait_for_recheck_completion
from .ssh_manager import (
    SSHConnectionPool, setup_ssh_control_path, check_sshpass_installed,
    _get_ssh_command, sftp_mkdir_p, get_remote_size_rsync, is_remote_dir,
    _get_all_files_recursive, batch_get_remote_sizes
)
import tempfile
import getpass
import configupdater
import os
import time
import argparse
import argcomplete
from rich.console import Console
from rich.table import Table
from rich.logging import RichHandler
from .ui import UIManagerV2 as UIManager
from typing import Optional, Dict, List, Tuple, Any, Set, TYPE_CHECKING
from .transfer_manager import (
    FileTransferTracker, TransferCheckpoint, transfer_content_rsync,
    transfer_content, transfer_content_sftp_upload, Timeouts
)

if TYPE_CHECKING:
    import qbittorrentapi

# --- Constants ---
MAX_RETRY_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 5
DEFAULT_PARALLEL_JOBS = 4
GB_BYTES = 1024**3

def get_remote_size(sftp: paramiko.SFTPClient, remote_path: str) -> int:
    """Recursively gets the total size of a remote file or directory."""
    total_size = 0
    try:
        stat = sftp.stat(remote_path)
        if stat.st_mode & 0o40000:  # S_ISDIR
            for item in sftp.listdir(remote_path):
                item_path = f"{remote_path.rstrip('/')}/{item}"
                total_size += get_remote_size(sftp, item_path)
        else:
            total_size = stat.st_size if stat.st_size is not None else 0
    except FileNotFoundError:
        logging.warning(f"Could not stat remote path for size calculation: {remote_path}")
        return 0
    return total_size

def cleanup_orphaned_cache(destination_qbit: qbittorrentapi.Client) -> None:
    """
    Scans for leftover cache directories from previous runs and removes them
    if the corresponding torrent is already successfully on the destination client.
    """
    temp_dir = Path(tempfile.gettempdir())
    cache_prefix = "torrent_mover_cache_"
    logging.info("--- Running Orphaned Cache Cleanup ---")
    try:
        all_dest_hashes: Set[str] = {t.hash for t in destination_qbit.torrents_info() if t.progress == 1}
        logging.info(f"Found {len(all_dest_hashes)} completed torrent hashes on the destination client for cleanup check.")
    except Exception as e:
        logging.error(f"Could not retrieve torrents from destination client for cache cleanup: {e}")
        return
    found_cache_dirs = 0
    cleaned_cache_dirs = 0
    for item in temp_dir.iterdir():
        if item.is_dir() and item.name.startswith(cache_prefix):
            found_cache_dirs += 1
            torrent_hash = item.name[len(cache_prefix):]
            if torrent_hash in all_dest_hashes:
                logging.warning(f"Found orphaned cache for a completed torrent: {torrent_hash}. Removing.")
                try:
                    shutil.rmtree(item)
                    cleaned_cache_dirs += 1
                except OSError as e:
                    logging.error(f"Failed to remove orphaned cache directory '{item}': {e}")
            else:
                logging.info(f"Found valid cache for an incomplete or non-existent torrent: {torrent_hash}. Leaving it for resume.")
    if found_cache_dirs == 0:
        logging.info("No leftover cache directories found.")
    else:
        logging.info(f"Cleanup complete. Removed {cleaned_cache_dirs}/{found_cache_dirs} orphaned cache directories.")

def recover_cached_torrents(source_qbit: qbittorrentapi.Client, destination_qbit: qbittorrentapi.Client) -> List[qbittorrentapi.TorrentDictionary]:
    """
    Scans for leftover cache directories from previous runs and adds them back
    to the processing queue if the torrent is not on the destination.
    """
    temp_dir = Path(tempfile.gettempdir())
    cache_prefix = "torrent_mover_cache_"
    logging.info("--- Checking for incomplete cached transfers to recover ---")
    try:
        all_dest_hashes: Set[str] = {t.hash for t in destination_qbit.torrents_info()}
        logging.info(f"Found {len(all_dest_hashes)} torrent hashes on the destination client for recovery check.")
    except Exception as e:
        logging.error(f"Could not retrieve torrents from destination client for cache recovery: {e}")
        return []
    cache_dirs = [item for item in temp_dir.iterdir() if item.is_dir() and item.name.startswith(cache_prefix)]
    if not cache_dirs:
        logging.info("No leftover cache directories found to recover.")
        return []
    logging.info(f"Found {len(cache_dirs)} cache directories to check for recovery.")
    hashes_to_recover = []
    for item in cache_dirs:
        torrent_hash = item.name[len(cache_prefix):]
        if torrent_hash not in all_dest_hashes:
            logging.warning(f"Found an incomplete cached transfer: {torrent_hash}. Will attempt to recover.")
            hashes_to_recover.append(torrent_hash)
    if not hashes_to_recover:
        logging.info("All found cache directories correspond to completed torrents.")
        return []
    try:
        source_torrents = source_qbit.torrents_info(torrent_hashes=hashes_to_recover)
        recovered_torrents = list(source_torrents)
        if recovered_torrents:
            logging.info(f"Successfully recovered {len(recovered_torrents)} torrent(s) from cache. They will be added to the queue.")
        else:
            logging.warning("Could not find matching torrents on the source for the incomplete cache directories.")
        return recovered_torrents
    except Exception as e:
        logging.error(f"An error occurred while trying to get torrent info from the source for recovery: {e}")
        return []

def destination_health_check(config: configparser.ConfigParser, total_transfer_size_bytes: int, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> bool:
    """
    Performs checks on the destination (local or remote) to ensure it's ready.
    """
    logging.info("--- Running Destination Health Check ---")
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    dest_path = config['DESTINATION_PATHS'].get('destination_path')
    remote_config = config['DESTINATION_SERVER'] if transfer_mode == 'sftp_upload' and 'DESTINATION_SERVER' in config else None
    dest_pool = ssh_connection_pools.get('DESTINATION_SERVER') if remote_config else None
    available_space = -1
    try:
        if remote_config and dest_pool:
            with dest_pool.get_connection() as (sftp, ssh):
                command = f"df -kP {shlex.quote(dest_path)}"
                stdin, stdout, stderr = ssh.exec_command(command, timeout=Timeouts.SSH_EXEC)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status != 0:
                    stderr_output = stderr.read().decode('utf-8').strip()
                    if "not found" in stderr_output or "no such" in stderr_output.lower():
                        raise FileNotFoundError(f"The 'df' command was not found on the remote server. Cannot check disk space.")
                    raise Exception(f"'df' command failed with exit code {exit_status}. Stderr: {stderr_output}")
                output = stdout.read().decode('utf-8').strip().splitlines()
                if len(output) < 2:
                    stderr_output = stderr.read().decode('utf-8').strip()
                    if "no such file or directory" in stderr_output.lower():
                        raise FileNotFoundError(f"The remote path '{dest_path}' does not exist.")
                    raise ValueError(f"Could not parse 'df' output. Raw output: '{' '.join(output)}'")
                parts = output[1].split()
                if len(parts) < 4:
                    raise ValueError(f"Unexpected 'df' output format. Line: '{output[1]}'")
                available_kb = int(parts[3])
                available_space = available_kb * 1024
        else:
            stats = os.statvfs(dest_path)
            available_space = stats.f_bavail * stats.f_frsize
        required_space_gb = total_transfer_size_bytes / GB_BYTES
        available_space_gb = available_space / GB_BYTES
        logging.info(f"Required space for this run: {required_space_gb:.2f} GB. Available on destination: {available_space_gb:.2f} GB.")
        if total_transfer_size_bytes > available_space:
            logging.error("FATAL: Not enough disk space on the destination.")
            logging.error(f"Required: {required_space_gb:.2f} GB, Available: {available_space_gb:.2f} GB.")
            return False
        else:
            logging.info("[green]SUCCESS:[/] Destination has enough disk space.")
    except FileNotFoundError:
        logging.error(f"FATAL: The destination path '{dest_path}' does not exist.")
        if not remote_config:
            logging.error("Please ensure the base directory is created and accessible on the local filesystem.")
        else:
            logging.error("Please ensure the base directory is created and accessible on the remote server.")
        return False
    except Exception as e:
        logging.error(f"An error occurred during disk space check: {e}", exc_info=True)
        return False
    if not test_path_permissions(dest_path, remote_config=remote_config, ssh_connection_pools=ssh_connection_pools):
        logging.error("FATAL: Destination permission check failed.")
        return False
    logging.info("--- Destination Health Check Passed ---")
    return True

def load_tracker_rules(script_dir: Path, rules_filename: str = "tracker_rules.json") -> Dict[str, str]:
    rules_file = script_dir / rules_filename
    if not rules_file.is_file():
        logging.warning(f"Tracker rules file not found at '{rules_file}'. Starting with empty ruleset.")
        return {}
    try:
        with open(rules_file, 'r') as f:
            rules = json.load(f)
        logging.info(f"Successfully loaded {len(rules)} tracker rules from '{rules_file}'.")
        return rules
    except json.JSONDecodeError:
        logging.error(f"Could not decode JSON from '{rules_file}'. Please check its format.")
        return {}

def save_tracker_rules(rules: Dict[str, str], script_dir: Path, rules_filename: str = "tracker_rules.json") -> bool:
    rules_file = script_dir / rules_filename
    try:
        with open(rules_file, 'w') as f:
            json.dump(rules, f, indent=4, sort_keys=True)
        logging.info(f"Successfully saved {len(rules)} rules to '{rules_file}'.")
        return True
    except Exception as e:
        logging.error(f"Failed to save rules to '{rules_file}': {e}")
        return False

def get_tracker_domain(tracker_url: str) -> Optional[str]:
    try:
        netloc = urlparse(tracker_url).netloc
        parts = netloc.split('.')
        if len(parts) > 2:
            if parts[0] in ['tracker', 'announce', 'www']:
                return '.'.join(parts[1:])
        return netloc
    except Exception:
        return None

def get_category_from_rules(torrent: qbittorrentapi.TorrentDictionary, rules: Dict[str, str], client: qbittorrentapi.Client) -> Optional[str]:
    try:
        trackers = client.torrents_trackers(torrent_hash=torrent.hash)
        for tracker in trackers:
            domain = get_tracker_domain(tracker.get('url'))
            if domain and domain in rules:
                return rules[domain]
    except Exception as e:
        logging.warning(f"Could not check trackers for torrent '{torrent.name}': {e}")
    return None

def set_category_based_on_tracker(client: qbittorrentapi.Client, torrent_hash: str, tracker_rules: Dict[str, str], dry_run: bool = False) -> None:
    try:
        torrent_info = client.torrents_info(torrent_hashes=torrent_hash)
        if not torrent_info:
            logging.warning(f"Could not find torrent {torrent_hash[:10]} on destination to categorize.")
            return
        torrent = torrent_info[0]
        category = get_category_from_rules(torrent, tracker_rules, client)
        if category:
            if category == "ignore":
                logging.info(f"Rule is to ignore torrent '{torrent.name}'. Doing nothing.")
                return
            if torrent.category == category:
                logging.info(f"Torrent '{torrent.name}' is already in the correct category '{category}'.")
                return
            logging.info(f"Rule found. Setting category to '{category}' for '{torrent.name}'.")
            if not dry_run:
                client.torrents_set_category(torrent_hashes=torrent.hash, category=category)
            else:
                logging.info(f"[DRY RUN] Would set category of '{torrent.name}' to '{category}'.")
        else:
            logging.info(f"No matching tracker rule found for torrent '{torrent.name}'.")
    except qbittorrentapi.exceptions.NotFound404Error:
        logging.warning(f"Torrent {torrent_hash[:10]} not found on destination when trying to categorize.")
    except Exception as e:
        logging.error(f"An error occurred during categorization for torrent {torrent_hash[:10]}: {e}", exc_info=True)

def change_ownership(path_to_change: str, user: str, group: str, remote_config: Optional[configparser.SectionProxy] = None, dry_run: bool = False, ssh_connection_pools: Optional[Dict[str, SSHConnectionPool]] = None) -> None:
    """
    Changes the ownership of a file or directory, either locally or remotely.
    """
    if not user and not group:
        return
    owner_spec = f"{user or ''}:{group or ''}".strip(':')
    if dry_run:
        logging.info(f"[DRY RUN] Would change ownership of '{path_to_change}' to '{owner_spec}'.")
        return
    if remote_config and ssh_connection_pools:
        logging.info(f"Attempting to change remote ownership of '{path_to_change}' to '{owner_spec}'...")
        pool = ssh_connection_pools.get('DESTINATION_SERVER')
        if not pool:
            logging.error("Could not find SSH pool for DESTINATION_SERVER.")
            return
        try:
            with pool.get_connection() as (sftp, ssh):
                remote_command = f"chown -R -- {shlex.quote(owner_spec)} {shlex.quote(path_to_change)}"
                logging.debug(f"Executing remote command: {remote_command}")
                stdin, stdout, stderr = ssh.exec_command(remote_command, timeout=Timeouts.SSH_EXEC)
                exit_status = stdout.channel.recv_exit_status()
                if exit_status == 0:
                    logging.info("Remote ownership changed successfully.")
                else:
                    stderr_output = stderr.read().decode('utf-8').strip()
                    logging.error(f"Failed to change remote ownership for '{path_to_change}'. Exit code: {exit_status}, Stderr: {stderr_output}")
        except Exception as e:
            logging.error(f"An exception occurred during remote chown: {e}", exc_info=True)
    else:
        logging.info(f"Attempting to change local ownership of '{path_to_change}' to '{owner_spec}'...")
        try:
            if shutil.which("chown") is None:
                logging.warning("'chown' command not found locally. Skipping ownership change.")
                return
            command = ["chown", "-R", owner_spec, path_to_change]
            process = subprocess.run(command, capture_output=True, text=True, check=False)
            if process.returncode == 0:
                logging.info("Local ownership changed successfully.")
            else:
                logging.error(f"Failed to change local ownership for '{path_to_change}'. Exit code: {process.returncode}, Stderr: {process.stderr.strip()}")
        except Exception as e:
            logging.error(f"An exception occurred during local chown: {e}", exc_info=True)

def _pre_transfer_setup(torrent: qbittorrentapi.TorrentDictionary, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> Tuple[List[Tuple[str, str]], str, str, str, int]:
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

def _execute_transfer(torrent: qbittorrentapi.TorrentDictionary, total_size: int, config: configparser.ConfigParser, ui: UIManager, file_tracker: FileTransferTracker, ssh_connection_pools: Dict[str, SSHConnectionPool], all_files: List[Tuple[str, str]], source_content_path: str, dest_content_path: str, dry_run: bool, sftp_chunk_size: int) -> None:
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

def _delete_destination_content(dest_content_path: str, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> None:
    """Deletes the content from the destination path, either locally or remotely."""
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
    is_remote = (transfer_mode == 'sftp_upload')

    logging.warning(f"Attempting to delete destination content: {dest_content_path}")
    try:
        if is_remote:
            # Remote deletion
            dest_pool = ssh_connection_pools.get('DESTINATION_SERVER')
            if not dest_pool:
                logging.error("Cannot delete remote content: Destination SSH pool not found.")
                return

            with dest_pool.get_connection() as (sftp, ssh):
                # Check if it's a directory or file
                try:
                    stat = sftp.stat(dest_content_path)
                    if stat.st_mode & 0o40000: # S_ISDIR
                        logging.debug("Destination is a directory. Using 'rm -rf'.")
                        command = f"rm -rf {shlex.quote(dest_content_path)}"
                        stdin, stdout, stderr = ssh.exec_command(command, timeout=Timeouts.SSH_EXEC)
                        exit_status = stdout.channel.recv_exit_status()
                        if exit_status != 0:
                            logging.error(f"Failed to delete remote directory: {stderr.read().decode()}")
                    else:
                        logging.debug("Destination is a file. Using 'sftp.remove'.")
                        sftp.remove(dest_content_path)
                except FileNotFoundError:
                    logging.warning(f"Destination content not found (already deleted?): {dest_content_path}")
        else:
            # Local deletion
            p = Path(dest_content_path)
            if p.is_dir():
                logging.debug("Destination is a directory. Using 'shutil.rmtree'.")
                shutil.rmtree(p)
            elif p.is_file():
                logging.debug("Destination is a file. Using 'os.remove'.")
                p.unlink()
            else:
                logging.warning(f"Destination content not found (already deleted?): {dest_content_path}")

        logging.info(f"Successfully deleted destination content: {dest_content_path}")

    except Exception as e:
        logging.error(f"An error occurred while deleting destination content: {e}", exc_info=True)


def _post_transfer_actions(torrent: qbittorrentapi.TorrentDictionary, source_qbit: qbittorrentapi.Client, destination_qbit: qbittorrentapi.Client, config: configparser.ConfigParser, tracker_rules: Dict[str, str], ssh_connection_pools: Dict[str, SSHConnectionPool], dest_content_path: str, destination_save_path: str, dry_run: bool, test_run: bool) -> bool:
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

def transfer_torrent(torrent: qbittorrentapi.TorrentDictionary, total_size: int, source_qbit: qbittorrentapi.Client, destination_qbit: qbittorrentapi.Client, config: configparser.ConfigParser, tracker_rules: Dict[str, str], ui: UIManager, file_tracker: FileTransferTracker, ssh_connection_pools: Dict[str, SSHConnectionPool], dry_run: bool = False, test_run: bool = False, sftp_chunk_size: int = 65536) -> Tuple[bool, float]:
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
            ssh_connection_pools, dest_content_path, destination_save_path, dry_run, test_run
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

def run_interactive_categorization(client: qbittorrentapi.Client, rules: Dict[str, str], script_dir: Path, category_to_scan: str, no_rules: bool = False) -> None:
    """Interactively categorize torrents based on tracker domains."""
    logging.info("Starting interactive categorization...")
    if no_rules:
        logging.warning("Ignoring existing rules for this session (--no-rules).")
    try:
        if not category_to_scan:
            logging.info("No category specified. Scanning for 'uncategorized' torrents.")
            torrents_to_check = client.torrents_info(filter='uncategorized', sort='name')
        else:
            logging.info(f"Scanning for torrents in category: '{category_to_scan}'")
            torrents_to_check = client.torrents_info(category=category_to_scan, sort='name')
        if not torrents_to_check:
            logging.info(f"No torrents found in '{category_to_scan or 'uncategorized'}' that need categorization.")
            return
        available_categories = sorted(list(client.torrent_categories.categories.keys()))
        if not available_categories:
            logging.error("No categories found on the destination client. Cannot perform categorization.")
            return
        updated_rules = rules.copy()
        rules_changed = False
        console = Console()
        manual_review_count = 0
        for torrent in torrents_to_check:
            auto_category = None
            if not no_rules:
                auto_category = get_category_from_rules(torrent, updated_rules, client)
            if auto_category:
                if auto_category == "ignore":
                    logging.info(f"Ignoring '{torrent.name}' based on existing 'ignore' rule.")
                elif torrent.category != auto_category:
                    logging.info(f"Rule found for '{torrent.name}'. Setting category to '{auto_category}'.")
                    try:
                        client.torrents_set_category(torrent_hashes=torrent.hash, category=auto_category)
                    except Exception as e:
                        logging.error(f"Failed to set category for '{torrent.name}': {e}", exc_info=True)
                continue
            if manual_review_count == 0:
                logging.info("Some torrents require manual review.")
            manual_review_count += 1
            console.print("-" * 60)
            console.print(f"Torrent needs categorization: [bold]{torrent.name}[/bold]")
            console.print(f"   Current Category: {torrent.category or 'None'}")
            trackers = client.torrents_trackers(torrent_hash=torrent.hash)
            torrent_domains = sorted(list(set(d for d in [get_tracker_domain(t.get('url')) for t in trackers] if d)))
            console.print(f"   Tracker Domains: {', '.join(torrent_domains) if torrent_domains else 'None found'}")
            console.print("\nPlease choose an action:")
            for i, cat in enumerate(available_categories):
                console.print(f"  {i+1}: Set category to '{cat}'")
            console.print("\n  s: Skip this torrent (no changes)")
            console.print("  i: Ignore this torrent's trackers permanently")
            console.print("  q: Quit interactive session")
            while True:
                choice = console.input("Enter your choice: ").lower()
                if choice == 'q':
                    if rules_changed:
                        save_tracker_rules(updated_rules, script_dir)
                    return
                if choice == 's':
                    break
                if choice == 'i':
                    if not torrent_domains:
                        console.print("No domains to ignore. Skipping.")
                        break
                    for domain in torrent_domains:
                        if domain not in updated_rules:
                            console.print(f"Creating 'ignore' rule for domain: {domain}")
                            updated_rules[domain] = "ignore"
                            rules_changed = True
                    break
                try:
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(available_categories):
                        chosen_category = available_categories[choice_idx]
                        console.print(f"Setting category to '{chosen_category}'.")
                        client.torrents_set_category(torrent_hashes=torrent.hash, category=chosen_category)
                        if torrent_domains:
                            while True:
                                learn = console.input("Create a rule for this choice? (y/n): ").lower()
                                if learn in ['y', 'yes']:
                                    if len(torrent_domains) == 1:
                                        domain_to_rule = torrent_domains[0]
                                        console.print(f"Creating rule: '{domain_to_rule}' -> '{chosen_category}'")
                                        updated_rules[domain_to_rule] = chosen_category
                                        rules_changed = True
                                        break
                                    else:
                                        console.print("Choose a domain for the rule:")
                                        for j, domain in enumerate(torrent_domains):
                                            console.print(f"  {j+1}: {domain}")
                                        domain_choice = console.input(f"Enter number (1-{len(torrent_domains)}): ")
                                        try:
                                            domain_idx = int(domain_choice) - 1
                                            if 0 <= domain_idx < len(torrent_domains):
                                                domain_to_rule = torrent_domains[domain_idx]
                                                console.print(f"Creating rule: '{domain_to_rule}' -> '{chosen_category}'")
                                                updated_rules[domain_to_rule] = chosen_category
                                                rules_changed = True
                                                break
                                            else:
                                                console.print("Invalid number.")
                                        except ValueError:
                                            console.print("Invalid input.")
                                elif learn in ['n', 'no']:
                                    break
                        break
                    else:
                        console.print("Invalid number. Please try again.")
                except ValueError:
                    console.print("Invalid input. Please enter a number or a valid command (s, i, q).")
        if rules_changed:
            save_tracker_rules(updated_rules, script_dir)
        console.print("-" * 60)
        logging.info("Interactive categorization session finished.")
    except Exception as e:
        logging.error(f"An error occurred during interactive categorization: {e}", exc_info=True)

def setup_logging(script_dir: Path, dry_run: bool, test_run: bool, debug: bool) -> None:
    """Configures logging to both console and a file."""
    log_dir = script_dir / 'logs'
    log_dir.mkdir(exist_ok=True)
    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    log_file_name = f"torrent_mover_{timestamp}.log"
    log_file_path = log_dir / log_file_name
    logger = logging.getLogger()
    log_level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(log_level)
    if logger.hasHandlers():
        logger.handlers.clear()
    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    rich_handler = RichHandler(level=log_level, show_path=False, rich_tracebacks=True, markup=True, console=Console(stderr=True))
    rich_formatter = logging.Formatter('%(message)s')
    rich_handler.setFormatter(rich_formatter)
    logger.addHandler(rich_handler)
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    logging.info("--- Torrent Mover script started ---")
    if dry_run:
        logging.warning("!!! DRY RUN MODE ENABLED. NO CHANGES WILL BE MADE. !!!")
    if test_run:
        logging.warning("!!! TEST RUN MODE ENABLED. SOURCE TORRENTS WILL NOT BE DELETED. !!!")

def pid_exists(pid: int) -> bool:
    """Check whether a process with the given PID exists."""
    if pid < 0: return False
    if pid == 0: return False
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH: return False
        elif err.errno == errno.EPERM: return True
        else: raise
    else:
        return True

def test_path_permissions(path_to_test: str, remote_config: Optional[configparser.SectionProxy] = None, ssh_connection_pools: Optional[Dict[str, SSHConnectionPool]] = None) -> bool:
    """
    Tests write permissions for a given path by creating and deleting a temporary file.
    """
    if remote_config:
        logging.info(f"--- Running REMOTE Permission Test on: {path_to_test} ---")
        if not ssh_connection_pools:
            logging.error("SSH connection pools not available for remote permission test.")
            return False
        pool = ssh_connection_pools.get('DESTINATION_SERVER')
        if not pool:
            logging.error("Could not find SSH pool for DESTINATION_SERVER.")
            return False
        try:
            with pool.get_connection() as (sftp, ssh):
                path_to_test = path_to_test.replace('\\', '/')
                test_file_path = f"{path_to_test.rstrip('/')}/permission_test_{os.getpid()}.tmp"
                logging.info(f"Attempting to create a remote test file: {test_file_path}")
                with sftp.open(test_file_path, 'w') as f:
                    f.write('test')
                logging.info("Remote test file created successfully.")
                sftp.remove(test_file_path)
            logging.info("Remote test file removed successfully.")
            logging.info(f"[bold green]SUCCESS:[/] Remote write permissions are correctly configured for '{remote_config['username']}' on path '{path_to_test}'")
            return True
        except PermissionError:
            logging.error(f"[bold red]FAILURE:[/] Permission denied on remote server when trying to write to '{path_to_test}'.\n"
                          f"Please ensure the user '{remote_config['username']}' has write access to this path on the remote server.")
            return False
        except Exception as e:
            logging.error(f"[bold red]FAILURE:[/] An unexpected error occurred during remote permission test: {e}", exc_info=True)
            return False
    else:
        p = Path(path_to_test)
        logging.info(f"--- Running LOCAL Permission Test on: {p} ---")
        if not p.exists():
            logging.error(f"Error: The local path '{p}' does not exist.\nPlease ensure the base directory is created and accessible.")
            return False
        if not p.is_dir():
            logging.error(f"Error: The local path '{p}' is not a directory.")
            return False
        test_file_path = p / f"permission_test_{os.getpid()}.tmp"
        try:
            logging.info(f"Attempting to create a test file: {test_file_path}")
            with open(test_file_path, 'w') as f:
                f.write('test')
            logging.info("Test file created successfully.")
            os.remove(test_file_path)
            logging.info("Test file removed successfully.")
            logging.info(f"[bold green]SUCCESS:[/] Local write permissions are correctly configured for {p}")
            return True
        except PermissionError:
            logging.error(f"[bold red]FAILURE:[/] Permission denied when trying to write to local path '{p}'.\n"
                          f"Please ensure the user '{getpass.getuser()}' running the script has write access to this path.")
            return False
        except Exception as e:
            logging.error(f"[bold red]FAILURE:[/] An unexpected error occurred during local permission test: {e}", exc_info=True)
            return False

def _handle_utility_commands(args: argparse.Namespace, config: configparser.ConfigParser, tracker_rules: Dict[str, str], script_dir: Path, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> bool:
    """Handles all utility command-line arguments that exit after running."""
    if not (args.list_rules or args.add_rule or args.delete_rule or args.interactive_categorize or args.test_permissions):
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
        if not tracker_rules:
            logging.info("No rules found.")
            return True
        console = Console()
        table = Table(title="Tracker to Category Rules", show_header=True, header_style="bold magenta")
        table.add_column("Tracker Domain", style="dim", width=40)
        table.add_column("Assigned Category")
        sorted_rules = sorted(tracker_rules.items())
        for domain, category in sorted_rules:
            table.add_row(domain, f"[yellow]{category}[/yellow]" if category == "ignore" else f"[cyan]{category}[/cyan]")
        console.print(table)
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

    if not eligible_torrents:
        logging.info("No torrents to move at this time.")
        return

    total_count = len(eligible_torrents)
    transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

    with UIManager(version=__version__) as ui:
        ui.set_transfer_mode(transfer_mode)
        ui.set_analysis_total(total_count)
        ui.log(f"Found {total_count} torrents to process. Analyzing...")
        sftp_config = config['SOURCE_SERVER']
        analyzed_torrents: List[Tuple[qbittorrentapi.TorrentDictionary, int]] = []
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
        ui.complete_analysis() # <-- ADD THIS LINE
        ui.log("Analysis complete. Verifying destination...")

        if not analyzed_torrents:
            ui.set_final_status("No valid, non-zero size torrents to transfer.")
            logging.info("No valid, non-zero size torrents to transfer.")
            time.sleep(2)
            return

        # Corrected logic for multiplier:
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
                        ui, file_tracker, ssh_connection_pools, args.dry_run, args.test_run, sftp_chunk_size
                    ): (t, size) for t, size in analyzed_torrents
                }
                for future in as_completed(transfer_futures):
                    torrent, size = transfer_futures[future]
                    try:
                        success, duration = future.result()
                        if success:
                            checkpoint.mark_completed(torrent.hash)
                            # Stats are now handled by complete_torrent_transfer
                        else:
                            pass # Stats handled by complete_torrent_transfer
                    except Exception as e:
                        logging.error(f"An exception was thrown for torrent '{torrent.name}': {e}", exc_info=True)
                        # We must still call complete_torrent_transfer to update stats
                        ui.complete_torrent_transfer(torrent.hash, success=False)
                # This task is no longer needed, as the UI doesn't have a "transfer" task bar
                # ui.advance_transfer_progress()
        except KeyboardInterrupt:
            ui.log("[bold yellow]Process interrupted by user. Transfers cancelled.[/]")
            ui.set_final_status("Shutdown requested.")
            raise

        with ui._lock: # Accessing stats safely
            completed_count = ui._stats['completed_transfers']
        ui.log(f"Processing complete. Moved {completed_count}/{total_count} torrent(s).")
        ui.set_final_status("All tasks finished.")
        with ui._lock: # Accessing stats safely
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
        if _handle_utility_commands(args, config, tracker_rules, script_dir, ssh_connection_pools):
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
