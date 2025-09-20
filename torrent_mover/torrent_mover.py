#!/usr/bin/env python3
# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

import configparser
import sys
import logging
from pathlib import Path
import qbittorrentapi
import paramiko
import json
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import defaultdict

def load_config(config_path="config.ini"):
    """
    Loads the configuration from the specified .ini file.
    Exits if the file is not found.
    """
    config_file = Path(config_path)
    if not config_file.is_file():
        logging.error(f"FATAL: Configuration file not found at '{config_path}'.")
        logging.error("Please copy 'config.ini.template' to 'config.ini' and fill in your details.")
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(config_file)
    return config

# --- Connection Functions ---

def connect_qbit(config_section, client_name):
    """
    Connects to a qBittorrent client using details from a config section.
    Returns a connected client object or None on failure.
    """
    try:
        host = config_section['host']
        port = config_section.getint('port')
        username = config_section['username']
        password = config_section['password']
        verify_cert = config_section.getboolean('verify_cert', fallback=True)

        logging.info(f"Connecting to {client_name} qBittorrent at {host}...")
        client = qbittorrentapi.Client(
            host=host,
            port=port,
            username=username,
            password=password,
            VERIFY_WEBUI_CERTIFICATE=verify_cert
        )
        client.auth_log_in()
        logging.info(f"Successfully connected to {client_name}. Version: {client.app.version}")
        return client
    except Exception as e:
        logging.error(f"Failed to connect to {client_name} qBittorrent: {e}")
        return None

def connect_sftp(config_section):
    """
    Connects to a server via SFTP using details from a config section.
    Returns a connected SFTP client and transport object, or (None, None) on failure.
    """
    try:
        host = config_section['host']
        port = config_section.getint('port')
        username = config_section['username']
        password = config_section['password']

        logging.info(f"Establishing SFTP connection to {host}...")
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logging.info(f"Successfully established SFTP connection to {host}.")
        return sftp, transport
    except Exception as e:
        logging.error(f"Failed to establish SFTP connection: {e}")
        return None, None

import os
import sys
import time
import argparse
import argcomplete
from rich.console import Group
from rich.panel import Panel
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
    TimeElapsedColumn,
    FileSizeColumn,
    TotalFileSizeColumn,
    MofNCompleteColumn,
)
from rich.live import Live
from rich.logging import RichHandler


# --- SFTP Transfer Logic with Progress Bar ---

class CustomProgress(Progress):
    """A custom Progress class that adds a method to get a live task."""
    def get_task(self, task_id):
        """Gets a live task object by its ID."""
        return self._tasks[task_id]

class DownloadProgress:
    """
    A thread-safe progress bar callback for Paramiko's SFTP get method.
    Updates all relevant Rich Progress tasks based on bytes transferred.
    """
    def __init__(self, job_progress, file_task_id, overall_progress, overall_task_id):
        self._job_progress = job_progress
        self._file_task_id = file_task_id
        self._overall_progress = overall_progress
        self._overall_task_id = overall_task_id
        self._last_bytes = 0

    def __call__(self, bytes_transferred, total_bytes):
        """
        The callback method invoked by Paramiko.
        Calculates the increment and updates all relevant progress bars.
        """
        increment = bytes_transferred - self._last_bytes
        self._job_progress.update(self._file_task_id, advance=increment)
        self._overall_progress.update(self._overall_task_id, advance=increment)
        self._last_bytes = bytes_transferred

def get_remote_size(sftp, remote_path):
    """Recursively gets the total size of a remote file or directory."""
    total_size = 0
    try:
        stat = sftp.stat(remote_path)
        if stat.st_mode & 0o40000:  # S_ISDIR
            for item in sftp.listdir(remote_path):
                item_path = f"{remote_path.rstrip('/')}/{item}"
                total_size += get_remote_size(sftp, item_path)
        else:
            total_size = stat.st_size
    except FileNotFoundError:
        logging.warning(f"Could not stat remote path for size calculation: {remote_path}")
        return 0
    return total_size

def _get_all_files_recursive(sftp, remote_path, local_path, file_list):
    """
    Recursively walks a remote directory to build a flat list of all files to download.
    """
    for item in sftp.listdir(remote_path):
        remote_item_path = f"{remote_path.rstrip('/')}/{item}"
        local_item_path = os.path.join(local_path, item)

        stat_info = sftp.stat(remote_item_path)
        if stat_info.st_mode & 0o40000:  # S_ISDIR
            _get_all_files_recursive(sftp, remote_item_path, local_item_path, file_list)
        else:
            file_list.append((remote_item_path, local_item_path))

def _sftp_download_file(sftp_config, remote_file, local_file, job_progress, parent_task_id, overall_progress, overall_task_id, parent_to_children_map, map_lock, dry_run=False):
    """
    Downloads a single file with a progress bar. Establishes its own SFTP session
    to ensure thread safety when called from a ThreadPoolExecutor.
    """
    local_path = Path(local_file)
    file_name = os.path.basename(remote_file)

    sftp = None
    transport = None
    try:
        sftp, transport = connect_sftp(sftp_config)
        if not sftp:
            raise Exception(f"Failed to establish SFTP connection for thread downloading {file_name}")

        remote_stat = sftp.stat(remote_file)
        total_size = remote_stat.st_size

        if total_size == 0:
            logging.warning(f"Skipping zero-byte file: {file_name}")
            return

        if local_path.exists():
            local_size = local_path.stat().st_size
            if local_size == total_size:
                logging.info(f"Skipping (exists and size matches): {file_name}")
                overall_progress.update(overall_task_id, advance=total_size)
                return
            else:
                logging.warning(f"Overwriting (size mismatch r:{total_size}/l:{local_size}): {file_name}")

        if dry_run:
            logging.info(f"[DRY RUN] Would download: {remote_file} -> {local_path}")
            overall_progress.update(overall_task_id, advance=total_size)
            return

        local_path.parent.mkdir(parents=True, exist_ok=True)

        file_task_id = job_progress.add_task(f"└─ [cyan]{file_name}[/]", total=total_size, start=True, transient=True)
        with map_lock:
            parent_to_children_map[parent_task_id].append(file_task_id)

        try:
            callback = DownloadProgress(job_progress, file_task_id, overall_progress, overall_task_id)
            sftp.get(remote_file, str(local_path), callback=callback)
            logging.info(f"Download of '{file_name}' completed.")
        except Exception as e:
            logging.error(f"Download failed for {file_name}: {e}")
            job_progress.update(file_task_id, description=f"[bold red]Failed: {file_name}[/]")
            raise
        finally:
            job_progress.update(file_task_id, visible=False)
            job_progress.remove_task(file_task_id)

    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()

def transfer_content(sftp_config, sftp, remote_path, local_path, job_progress, parent_task_id, overall_progress, overall_task_id, parent_to_children_map, map_lock, dry_run=False):
    """
    Transfers a remote file or directory to a local path, preserving structure.
    Handles directories by downloading their files concurrently.
    """
    remote_stat = sftp.stat(remote_path)
    if remote_stat.st_mode & 0o40000:  # S_ISDIR
        logging.info(f"Directory detected. Finding all files for concurrent download...")
        all_files = []
        _get_all_files_recursive(sftp, remote_path, local_path, all_files)

        MAX_CONCURRENT_FILES = 5
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_FILES) as file_executor:
            futures = [
                file_executor.submit(_sftp_download_file, sftp_config, remote_f, local_f, job_progress, parent_task_id, overall_progress, overall_task_id, parent_to_children_map, map_lock, dry_run)
                for remote_f, local_f in all_files
            ]
            for future in as_completed(futures):
                future.result()

    else: # It's a single file
        _sftp_download_file(sftp_config, remote_path, local_path, job_progress, parent_task_id, overall_progress, overall_task_id, parent_to_children_map, map_lock, dry_run)

# --- Torrent Processing Logic ---

def get_eligible_torrents(client, category):
    """Gets a list of completed torrents from the client that match the specified category."""
    try:
        torrents = client.torrents_info(category=category, status_filter='completed')
        logging.info(f"Found {len(torrents)} completed torrent(s) in category '{category}'.")
        return torrents
    except Exception as e:
        logging.error(f"Could not retrieve torrents from client: {e}")
        return []

def wait_for_recheck_completion(client, torrent_hash, timeout_seconds=900, dry_run=False):
    """Waits for a torrent to complete its recheck. Returns True on success."""
    if dry_run:
        logging.info(f"[DRY RUN] Would wait for recheck on {torrent_hash[:10]}. Assuming success.")
        return True

    start_time = time.time()
    logging.info(f"Waiting for recheck to complete for torrent {torrent_hash[:10]}...")
    while time.time() - start_time < timeout_seconds:
        try:
            torrent_info = client.torrents_info(torrent_hashes=torrent_hash)
            if not torrent_info:
                logging.warning(f"Torrent {torrent_hash[:10]} disappeared while waiting for recheck.")
                return False
            torrent = torrent_info[0]
            if torrent.progress == 1:
                logging.info(f"Recheck completed for torrent {torrent_hash[:10]}.")
                return True
            time.sleep(10)
        except Exception as e:
            logging.error(f"Error while waiting for recheck on {torrent_hash[:10]}: {e}")
            return False
    logging.error(f"Timeout: Recheck did not complete for torrent {torrent_hash[:10]} in {timeout_seconds}s.")
    return False

# --- Tracker-based Categorization ---

def load_tracker_rules(script_dir, rules_filename="tracker_rules.json"):
    """
    Loads tracker-to-category rules from the specified JSON file.
    Returns an empty dictionary if the file doesn't exist or is invalid.
    """
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

def save_tracker_rules(rules, script_dir, rules_filename="tracker_rules.json"):
    """Saves the tracker rules dictionary to the specified JSON file."""
    rules_file = script_dir / rules_filename
    try:
        with open(rules_file, 'w') as f:
            json.dump(rules, f, indent=4, sort_keys=True)
        logging.info(f"Successfully saved {len(rules)} rules to '{rules_file}'.")
        return True
    except Exception as e:
        logging.error(f"Failed to save rules to '{rules_file}': {e}")
        return False

def get_tracker_domain(tracker_url):
    """Extracts the network location (domain) from a tracker URL."""
    try:
        netloc = urlparse(tracker_url).netloc
        # Simple subdomain stripping for better matching
        parts = netloc.split('.')
        if len(parts) > 2:
            # e.g., tracker.site.com -> site.com, announce.site.org -> site.org
            if parts[0] in ['tracker', 'announce', 'www']:
                return '.'.join(parts[1:])
        return netloc
    except Exception:
        return None

def get_category_from_rules(torrent, rules, client):
    """
    Checks a torrent's trackers against the rules to find a matching category.
    Returns the category name or None if no rule matches.
    """
    try:
        trackers = client.torrents_trackers(torrent_hash=torrent.hash)
        for tracker in trackers:
            domain = get_tracker_domain(tracker.get('url'))
            if domain and domain in rules:
                return rules[domain]
    except Exception as e:
        logging.warning(f"Could not check trackers for torrent '{torrent.name}': {e}")
    return None

def set_category_based_on_tracker(client, torrent_hash, tracker_rules, dry_run=False):
    """
    Sets a torrent's category based on its trackers and the predefined rules.
    """
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


def process_torrent(torrent, mandarin_qbit, unraid_qbit, sftp_config, config, tracker_rules, job_progress, overall_progress, overall_task_id, parent_to_children_map, map_lock, dry_run=False, test_run=False):
    """
    Executes the full transfer and management process for a single torrent.
    Establishes its own SFTP connection to be thread-safe.
    """
    name, hash = torrent.name, torrent.hash

    sftp = None
    transport = None
    source_paused = False

    parent_task_id = job_progress.add_task(f"{name}", total=1, start=False, visible=True)

    try:
        sftp, transport = connect_sftp(sftp_config)
        if not sftp:
            raise Exception("Failed to establish SFTP connection for this thread.")

        source_base_path = config['MANDARIN_SFTP']['source_path']
        dest_base_path = config['UNRAID_PATHS']['destination_path']
        remote_dest_base_path = config['UNRAID_PATHS'].get('remote_destination_path') or dest_base_path

        remote_content_path = torrent.content_path
        if not remote_content_path.startswith(source_base_path):
            raise ValueError(f"Content path '{remote_content_path}' not inside source path '{source_base_path}'.")

        total_size = get_remote_size(sftp, remote_content_path)
        if total_size == 0:
            logging.warning(f"Skipping torrent with no content or zero size: {name}")
            job_progress.update(parent_task_id, description=f"[yellow]Skipped (zero size): {name}[/]")
            return True

        job_progress.update(parent_task_id, total=total_size, start=True)

        relative_path = os.path.relpath(remote_content_path, source_base_path)
        local_dest_path = os.path.join(dest_base_path, relative_path)

        logging.info(f"Starting SFTP transfer for '{name}'")
        transfer_content(sftp_config, sftp, remote_content_path, local_dest_path, job_progress, parent_task_id, overall_progress, overall_task_id, parent_to_children_map, map_lock, dry_run)
        logging.info(f"SFTP transfer completed successfully for '{name}'.")

        # 2. Add to Unraid, paused
        unraid_save_path = os.path.join(remote_dest_base_path, os.path.dirname(relative_path))
        unraid_save_path = unraid_save_path.replace("\\", "/") # Ensure forward slashes for cross-platform compatibility

        if not dry_run:
            # Export the .torrent file from the source client
            logging.info(f"Exporting .torrent file for {name}")
            torrent_file_content = mandarin_qbit.torrents_export(torrent_hash=hash)

            logging.info(f"Adding torrent to Unraid (paused) with save path '{unraid_save_path}': {name}")
            unraid_qbit.torrents_add(
                torrent_files=torrent_file_content,
                save_path=unraid_save_path,
                is_paused=True,
                category=torrent.category
            )
            time.sleep(5)
        else:
            logging.info(f"[DRY RUN] Would export and add torrent to Unraid (paused) with save path '{unraid_save_path}': {name}")

        # 3. Force recheck on Unraid
        if not dry_run:
            logging.info(f"Triggering force recheck on Unraid for: {name}")
            unraid_qbit.torrents_recheck(torrent_hashes=hash)
        else:
            logging.info(f"[DRY RUN] Would trigger force recheck on Unraid for: {name}")

        # 4. Wait for recheck and then start
        if wait_for_recheck_completion(unraid_qbit, hash, dry_run=dry_run):
            # 5. Start destination torrent
            if not dry_run:
                logging.info(f"Starting torrent on Unraid: {name}")
                unraid_qbit.torrents_resume(torrent_hashes=hash)
            else:
                logging.info(f"[DRY RUN] Would start torrent on Unraid: {name}")

            # 6. Set category based on tracker rules
            logging.info(f"Attempting to categorize torrent on Unraid: {name}")
            set_category_based_on_tracker(unraid_qbit, hash, tracker_rules, dry_run=dry_run)

            # 7. Pause source torrent on Mandarin before deletion
            if not dry_run and not test_run:
                logging.info(f"Pausing torrent on Mandarin before deletion: {name}")
                mandarin_qbit.torrents_pause(torrent_hashes=hash)
                source_paused = True
            else:
                logging.info(f"[DRY RUN/TEST RUN] Would pause torrent on Mandarin: {name}")

            # 8. Delete from Mandarin
            if test_run:
                logging.info(f"[TEST RUN] Skipping deletion of torrent from Mandarin: {name}")
            elif not dry_run:
                logging.info(f"Deleting torrent and data from Mandarin: {name}")
                mandarin_qbit.torrents_delete(torrent_hashes=hash, delete_files=True)
            else:
                logging.info(f"[DRY RUN] Would delete torrent and data from Mandarin: {name}")

            logging.info(f"--- Successfully processed torrent: {name} ---")
            return True
        else:
            logging.error(f"Failed to verify recheck for {name}. Leaving on Mandarin for next run.")
            return False
    except Exception as e:
        logging.error(f"An error occurred while processing torrent {name}: {e}", exc_info=True)
        job_progress.update(parent_task_id, description=f"[bold red]Failed: {name}[/]")
        if not dry_run and source_paused:
            try:
                mandarin_qbit.torrents_resume(torrent_hashes=hash)
            except Exception as resume_e:
                logging.error(f"Failed to resume torrent {name} on Mandarin after error: {resume_e}")
        return False
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()
        job_progress.update(parent_task_id, visible=False)

# --- UI and Main Execution ---

def format_speed(speed_bytes_per_sec):
    """Formats bytes/sec speed into a human-readable string with appropriate units."""
    if not isinstance(speed_bytes_per_sec, (int, float)) or speed_bytes_per_sec < 0:
        return "0 B/s"
    if speed_bytes_per_sec < 1024:
        return f"{speed_bytes_per_sec:,.0f} B/s"
    if speed_bytes_per_sec < 1024**2:
        return f"{speed_bytes_per_sec / 1024:,.1f} KiB/s"
    if speed_bytes_per_sec < 1024**3:
        return f"{speed_bytes_per_sec / (1024**2):,.1f} MiB/s"
    return f"{speed_bytes_per_sec / (1024**3):,.1f} GiB/s"

def format_time(seconds):
    """Formats a duration in seconds into a human-readable H:MM:SS string."""
    if seconds is None or seconds == float('inf') or seconds < 0:
        return "-:--:--"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{int(h):d}:{int(m):02d}:{int(s):02d}"

def _ui_updater(job_progress, parent_to_children_map, map_lock, stop_event):
    """
    A background thread that periodically updates the description of parent torrent tasks
    with aggregate statistics (speed, remaining time) from their child file tasks.
    """
    original_names = {}

    while not stop_event.is_set():
        with map_lock:
            for parent_id, children_ids in list(parent_to_children_map.items()):
                try:
                    if parent_id not in original_names:
                        # Get the parent task to store its original name
                        original_names[parent_id] = job_progress.get_task(parent_id).description

                    # Get live task objects using the new method
                    child_tasks = [job_progress.get_task(child_id) for child_id in children_ids]

                    total_completed = sum(task.completed for task in child_tasks)
                    total_speed = sum(task.speed for task in child_tasks if task.speed is not None)

                    parent_task = job_progress.get_task(parent_id)
                    job_progress.update(parent_id, completed=total_completed)

                    time_remaining_str = ""
                    if total_speed > 0:
                        remaining_bytes = parent_task.total - total_completed
                        time_rem = remaining_bytes / total_speed if remaining_bytes > 0 else 0
                        time_remaining_str = format_time(time_rem)
                    else:
                        time_remaining_str = "-:--:--"

                    speed_str = format_speed(total_speed)

                    name = original_names.get(parent_id, "")
                    new_description = f"[bold cyan]{name}[/]\n  └─ Aggregated: [yellow]{speed_str}[/] | [magenta]{time_remaining_str}[/]"
                    job_progress.update(parent_id, description=new_description)
                except KeyError:
                    # A task was likely removed, which is fine.
                    continue
        time.sleep(0.5)

def run_interactive_categorization(client, rules, script_dir, category_to_scan):
    """
    Interactively prompts the user to categorize torrents that do not match any existing rules.
    """
    logging.info("Starting interactive categorization...")
    try:
        torrents_to_check = client.torrents_info(category=category_to_scan, sort='name')
        available_categories = sorted(list(client.torrent_categories.categories.keys()))

        if not available_categories:
            logging.error("No categories found on the destination client. Cannot perform categorization.")
            return

        updated_rules = rules.copy()
        rules_changed = False

        for torrent in torrents_to_check:
            # Check if the torrent already has a category based on existing rules
            auto_category = get_category_from_rules(torrent, updated_rules, client)
            if auto_category:
                if auto_category == "ignore":
                    print(f" -> Ignoring '{torrent.name}' based on rule.")
                else:
                    print(f" -> Rule found for '{torrent.name}'. Setting category to '{auto_category}'.")
                    try:
                        client.torrents_set_category(torrent_hashes=torrent.hash, category=auto_category)
                    except Exception as e:
                        logging.error(f"Failed to set category for '{torrent.name}': {e}", exc_info=True)
                        print(f"    ERROR: Could not set category for '{torrent.name}'. See log for details.")
                continue # Move to the next torrent

            # --- If no rule is found, start the interactive prompt ---
            print("-" * 60)
            print(f"Torrent needs categorization: {torrent.name}")
            print(f"   Current Category: {torrent.category or 'None'}")

            trackers = client.torrents_trackers(torrent_hash=torrent.hash)
            torrent_domains = sorted(list(set(d for d in [get_tracker_domain(t.get('url')) for t in trackers] if d)))
            print(f"   Tracker Domains: {', '.join(torrent_domains) if torrent_domains else 'None found'}")

            print("\nPlease choose an action:")
            for i, cat in enumerate(available_categories):
                print(f"  {i+1}: Set category to '{cat}'")
            print("\n  s: Skip this torrent (no changes)")
            print("  i: Ignore this torrent's trackers permanently")
            print("  q: Quit interactive session")

            while True: # Loop for user input
                choice = input("Enter your choice: ").lower()
                if choice == 'q':
                    if rules_changed:
                        save_tracker_rules(updated_rules, script_dir)
                    return
                if choice == 's':
                    break # Skips to the next torrent

                if choice == 'i':
                    if not torrent_domains:
                        print("No domains to ignore. Skipping.")
                        break
                    for domain in torrent_domains:
                        if domain not in updated_rules:
                            print(f"Creating 'ignore' rule for domain: {domain}")
                            updated_rules[domain] = "ignore"
                            rules_changed = True
                    break

                try:
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(available_categories):
                        chosen_category = available_categories[choice_idx]
                        print(f"Setting category to '{chosen_category}'.")
                        client.torrents_set_category(torrent_hashes=torrent.hash, category=chosen_category)

                        # Ask to create a rule
                        if torrent_domains:
                            while True:
                                learn = input("Create a rule for this choice? (y/n): ").lower()
                                if learn in ['y', 'yes']:
                                    if len(torrent_domains) == 1:
                                        domain_to_rule = torrent_domains[0]
                                        print(f"Creating rule: '{domain_to_rule}' -> '{chosen_category}'")
                                        updated_rules[domain_to_rule] = chosen_category
                                        rules_changed = True
                                        break
                                    else:
                                        print("Choose a domain for the rule:")
                                        for j, domain in enumerate(torrent_domains):
                                            print(f"  {j+1}: {domain}")
                                        domain_choice = input(f"Enter number (1-{len(torrent_domains)}): ")
                                        try:
                                            domain_idx = int(domain_choice) - 1
                                            if 0 <= domain_idx < len(torrent_domains):
                                                domain_to_rule = torrent_domains[domain_idx]
                                                print(f"Creating rule: '{domain_to_rule}' -> '{chosen_category}'")
                                                updated_rules[domain_to_rule] = chosen_category
                                                rules_changed = True
                                                break
                                            else:
                                                print("Invalid number.")
                                        except ValueError:
                                            print("Invalid input.")
                                elif learn in ['n', 'no']:
                                    break
                        break # Exit input loop
                    else:
                        print("Invalid number. Please try again.")
                except ValueError:
                    print("Invalid input. Please enter a number or a valid command (s, i, q).")

        if rules_changed:
            save_tracker_rules(updated_rules, script_dir)

        print("-" * 60)
        logging.info("Interactive categorization session finished.")

    except Exception as e:
        logging.error(f"An error occurred during interactive categorization: {e}", exc_info=True)

def main():
    """Main entry point for the script."""
    script_dir = Path(__file__).resolve().parent
    default_config_path = script_dir / 'config.ini'

    parser = argparse.ArgumentParser(
        description="A script to move qBittorrent torrents and data between servers.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--config', default=str(default_config_path), help='Path to the configuration file.')
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--dry-run', action='store_true', help='Simulate the process without making any changes.')
    mode_group.add_argument('--test-run', action='store_true', help='Run the full process but do not delete the source torrent.')
    parser.add_argument('--parallel-jobs', type=int, default=4, metavar='N', help='Number of torrents to process in parallel.')
    parser.add_argument('--list-rules', action='store_true', help='List all tracker-to-category rules and exit.')
    parser.add_argument('--add-rule', nargs=2, metavar=('TRACKER_DOMAIN', 'CATEGORY'), help='Add or update a rule and exit.')
    parser.add_argument('--delete-rule', metavar='TRACKER_DOMAIN', help='Delete a rule and exit.')
    parser.add_argument('--interactive-categorize', action='store_true', help='Interactively categorize torrents on destination without a rule.')

    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(message)s', handlers=[RichHandler(show_path=False, rich_tracebacks=True, markup=True)])

    tracker_rules = load_tracker_rules(script_dir)

    # --- Handle Rule Management (no Rich UI needed) ---
    if args.list_rules or args.add_rule or args.delete_rule:
        return 0

    # --- Handle Interactive Mode (uses its own print/input) ---
    if args.interactive_categorize:
        run_interactive_categorization(unraid_qbit, tracker_rules, script_dir, args.interactive_categorize)
        return 0

    logging.info("--- Torrent Mover script started ---")
    if args.dry_run:
        logging.warning("!!! DRY RUN MODE ENABLED. NO CHANGES WILL BE MADE. !!!")
    if args.test_run:
        logging.warning("!!! TEST RUN MODE ENABLED. SOURCE TORRENTS WILL NOT BE DELETED. !!!")

    config = load_config(args.config)
    mandarin_qbit = connect_qbit(config['MANDARIN_QBIT'], "Mandarin")
    unraid_qbit = connect_qbit(config['UNRAID_QBIT'], "Unraid")

    if not all([mandarin_qbit, unraid_qbit]):
        logging.error("One or more qBittorrent connections failed. Aborting.")
        return 1

    logging.info("qBittorrent connections established successfully.")

    stop_event = threading.Event()
    ui_thread = None
    try:
        category_to_move = config['SETTINGS']['category_to_move']
        eligible_torrents = get_eligible_torrents(mandarin_qbit, category_to_move)

        if not eligible_torrents:
            logging.info("No torrents to move at this time.")
            return 0

        total_count = len(eligible_torrents)
        processed_count = 0

        # --- Rich UI Layout ---
        torrent_progress = Progress(
            TextColumn("[bold blue]Torrents"),
            BarColumn(),
            MofNCompleteColumn(),
        )
        torrent_task = torrent_progress.add_task("Processing torrents", total=total_count)

        overall_progress = Progress(
            TextColumn("[bold green]Overall"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TotalFileSizeColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        )
        overall_task = overall_progress.add_task("Total progress", total=0)

        job_progress = CustomProgress(
            TextColumn("  {task.description}", justify="left"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TotalFileSizeColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
        )

        layout = Group(
            Panel(torrent_progress, title="Torrent Queue", border_style="blue"),
            Panel(overall_progress, title="Total Progress", border_style="green"),
            Panel(job_progress, title="Active Transfers", border_style="yellow", padding=(1, 2))
        )

        with Live(layout, refresh_per_second=10) as live:
            sftp_config = config['MANDARIN_SFTP']
            live.console.log("Calculating total size of all eligible torrents...")
            sftp, transport = connect_sftp(sftp_config)
            if not sftp:
                live.console.log("[bold red]Failed to establish a preliminary SFTP connection. Aborting.[/]")
                return 1

            grand_total_size = 0
            source_base_path = sftp_config['source_path']
            for torrent in eligible_torrents:
                remote_content_path = torrent.content_path
                if not remote_content_path.startswith(source_base_path):
                    live.console.log(f"[yellow]Warning: Skipping size calculation for torrent with invalid path: {torrent.name}[/]")
                    continue
                grand_total_size += get_remote_size(sftp, remote_content_path)

            overall_progress.update(overall_task, total=grand_total_size, description="[bold green]Overall")

            sftp.close()
            transport.close()
            live.console.log(f"Total size to transfer: [bold yellow]{grand_total_size/1024/1024/1024:.2f} GB[/]")

            parent_to_children_map = defaultdict(list)
            map_lock = threading.Lock()

            ui_thread = threading.Thread(
                target=_ui_updater,
                args=(job_progress, parent_to_children_map, map_lock, stop_event),
                daemon=True
            )
            ui_thread.start()

            with ThreadPoolExecutor(max_workers=args.parallel_jobs) as executor:
                future_to_torrent = {
                    executor.submit(process_torrent, torrent, mandarin_qbit, unraid_qbit, sftp_config, config, tracker_rules, job_progress, overall_progress, overall_task, parent_to_children_map, map_lock, args.dry_run, args.test_run): torrent
                    for torrent in eligible_torrents
                }

                for future in as_completed(future_to_torrent):
                    torrent = future_to_torrent[future]
                    try:
                        if future.result():
                            processed_count += 1
                    except Exception as e:
                        live.console.log(f"[bold red]An exception was thrown for torrent '{torrent.name}': {e}[/]", exc_info=True)
                    finally:
                        torrent_progress.update(torrent_task, advance=1)

        logging.info(f"Processing complete. Successfully moved {processed_count}/{total_count} torrent(s).")

    except KeyboardInterrupt:
        logging.info("\n[yellow]Process interrupted by user. Shutting down...[/yellow]")
    except KeyError as e:
        logging.error(f"Configuration key missing: {e}. Please check your config.ini.")
        return 1
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)
        return 1
    finally:
        stop_event.set()
        if ui_thread is not None:
            ui_thread.join()

    logging.info("--- Torrent Mover script finished ---")
    return 0

if __name__ == "__main__":
    sys.exit(main())
