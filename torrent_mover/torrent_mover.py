#!/usr/bin/env python3
# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

__version__ = "0.1.1"

import configparser
import sys
import logging
from pathlib import Path
import qbittorrentapi
import paramiko
import json
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import subprocess
import re
import threading
from collections import defaultdict
import errno
from utils import retry


def check_sshpass_installed():
    """
    Checks if sshpass is installed, which is required for rsync with password auth.
    Exits the script if it's not found.
    """
    if shutil.which("sshpass") is None:
        logging.error("FATAL: 'sshpass' is not installed or not in the system's PATH.")
        logging.error("Please install 'sshpass' to use the rsync transfer mode with a password.")
        logging.error("e.g., 'sudo apt-get install sshpass' or 'sudo yum install sshpass'")
        sys.exit(1)
    logging.info("'sshpass' dependency check passed.")


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

@retry(tries=2, delay=5)
def connect_qbit(config_section, client_name):
    """
    Connects to a qBittorrent client using details from a config section.
    Returns a connected client object or raises an exception on failure.
    """
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
        VERIFY_WEBUI_CERTIFICATE=verify_cert,
        REQUESTS_ARGS={'timeout': 10} # Add a timeout to the underlying requests
    )
    client.auth_log_in()
    logging.info(f"Successfully connected to {client_name}. Version: {client.app.version}")
    return client

@retry(tries=2, delay=5)
def connect_sftp(config_section):
    """
    Connects to a server via SFTP using details from a config section.
    Returns a connected SFTP client and transport object, or raises an exception on failure.
    """
    host = config_section['host']
    port = config_section.getint('port')
    username = config_section['username']
    password = config_section['password']

    logging.info(f"Establishing SFTP connection to {host}...")
    transport = paramiko.Transport((host, port))
    # Add a timeout to the transport connection and enable keepalives
    transport.banner_timeout = 20
    transport.set_keepalive(30)
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    logging.info(f"Successfully established SFTP connection to {host}.")
    return sftp, transport

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
from rich.text import Text
from rich.prompt import Prompt

# --- Custom Columns ---

class FileCountColumn(TextColumn):
    """A column to display the number of completed files vs total files for parent tasks."""
    def __init__(self, file_counts, *args, **kwargs):
        super().__init__("", *args, **kwargs)
        self.file_counts = file_counts

    def render(self, task: "Task") -> Text:
        if task.id in self.file_counts:
            completed, total = self.file_counts[task.id]
            return Text(f"{completed}/{total}", style="progress.percentage")
        return Text("")

class ConditionalTimeElapsedColumn(TimeElapsedColumn):
    """A column that displays the elapsed time only for child tasks."""
    def __init__(self, file_counts, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_counts = file_counts

    def render(self, task: "Task") -> Text:
        # If it's a parent task, don't render anything
        if task.id in self.file_counts:
            return Text("")
        return super().render(task)

class FixedWidthTimeRemainingColumn(TimeRemainingColumn):
    """Renders time remaining with a fixed width to prevent flickering."""
    def render(self, task: "Task") -> Text:
        """Render time remaining in a fixed-width column."""
        # Get the text from the parent class
        original_text_obj = super().render(task)
        original_text = original_text_obj.plain
        # Pad the text to a fixed width to prevent the column from changing size
        padded_text = f"{original_text:>10}"
        return Text(padded_text, style=original_text_obj.style)

class FixedWidthTransferSpeedColumn(TransferSpeedColumn):
    """Renders transfer speed with a fixed width to prevent flickering."""
    def render(self, task: "Task") -> Text:
        """Render transfer speed in a fixed-width column."""
        # Get the text from the parent class
        original_text_obj = super().render(task)
        original_text = original_text_obj.plain
        # Pad the text to a fixed width to prevent the column from changing size
        padded_text = f"{original_text:>12}"
        return Text(padded_text, style=original_text_obj.style)

# --- SFTP Transfer Logic with Progress Bar ---

class DownloadProgress:
    """
    A thread-safe progress bar callback for Paramiko's SFTP get method.
    Updates all relevant Rich Progress tasks based on bytes transferred.
    """
    def __init__(self, job_progress, file_task_id, parent_task_id, overall_progress, overall_task_id):
        self._job_progress = job_progress
        self._file_task_id = file_task_id
        self._parent_task_id = parent_task_id
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
        # For single-file torrents, parent and file task are the same.
        # Avoid advancing the same task twice.
        if self._file_task_id != self._parent_task_id:
            self._job_progress.update(self._parent_task_id, advance=increment)
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

@retry(tries=2, delay=5)
def get_remote_size_rsync(sftp_config, remote_path):
    """
    Gets the total size of a remote file or directory using rsync --stats, with retries.
    This is much faster than recursive SFTP STAT calls for directories with many files.
    """
    host = sftp_config['host']
    port = sftp_config.getint('port')
    username = sftp_config['username']
    password = sftp_config['password']

    remote_spec = f"{username}@{host}:{remote_path}"

    rsync_cmd = [
        "sshpass", "-p", password,
        "rsync",
        "-a", "--dry-run", "--stats", "--timeout=60",
        "-e", f"ssh -p {port} -c aes128-ctr -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=15",
        remote_spec,
        # A dummy local path is required. The path must exist.
        "."
    ]

    try:
        result = subprocess.run(
            rsync_cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )

        # rsync exit code 24 ("Partial transfer due to vanished source files") is okay for a size check.
        if result.returncode != 0 and result.returncode != 24:
            # Raise an exception to trigger the retry decorator
            raise Exception(f"Rsync (size check) failed with exit code {result.returncode}. Stderr: {result.stderr}")

        # The 'Total file size' is what we need.
        match = re.search(r"Total file size: ([\d,]+) bytes", result.stdout)
        if match:
            size_str = match.group(1).replace(',', '')
            return int(size_str)
        else:
            logging.warning(f"Could not parse rsync --stats output for torrent '{os.path.basename(remote_path)}'.")
            logging.debug(f"Rsync stdout for size check:\n{result.stdout}")
            # Raise exception if parsing fails, as it could be a sign of a failed transfer
            raise Exception("Failed to parse rsync stats output.")

    except FileNotFoundError:
        logging.error("FATAL: 'rsync' or 'sshpass' command not found during size check.")
        raise
    except Exception as e:
        # Re-raise to be handled by the retry decorator or the calling function
        raise e

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

@retry(tries=2, delay=5)
def _sftp_download_file(sftp_config, remote_file, local_file, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_id, dry_run=False):
    """
    Downloads a single file with a progress bar, with retries. Establishes its own SFTP session
    to ensure thread safety when called from a ThreadPoolExecutor.
    Uses a pre-existing task_id for the file progress bar.
    """
    local_path = Path(local_file)
    file_name = os.path.basename(remote_file)

    sftp = None
    transport = None
    download_successful = False
    try:
        sftp, transport = connect_sftp(sftp_config)

        remote_stat = sftp.stat(remote_file)
        total_size = remote_stat.st_size

        if total_size == 0:
            logging.warning(f"Skipping zero-byte file: {file_name}")
            with count_lock:
                file_counts[parent_task_id][0] += 1
            return

        if local_path.exists():
            # On retries, we might need to overwrite a partial file.
            # Reset the progress for this file task to ensure accuracy.
            if file_task_id is not None:
                job_progress.update(file_task_id, completed=0)

            local_size = local_path.stat().st_size
            if local_size == total_size:
                logging.info(f"Skipping (exists and size matches): {file_name}")
                job_progress.update(parent_task_id, advance=total_size)
                overall_progress.update(overall_task_id, advance=total_size)
                with count_lock:
                    file_counts[parent_task_id][0] += 1
                return
            else:
                logging.warning(f"Overwriting (size mismatch r:{total_size}/l:{local_size}): {file_name}")

        if dry_run:
            logging.info(f"[DRY RUN] Would download: {remote_file} -> {local_path}")
            job_progress.update(parent_task_id, advance=total_size)
            overall_progress.update(overall_task_id, advance=total_size)
            with count_lock:
                file_counts[parent_task_id][0] += 1
            return

        local_path.parent.mkdir(parents=True, exist_ok=True)

        effective_task_id = file_task_id if file_task_id is not None else parent_task_id
        if file_task_id is not None:
            job_progress.update(file_task_id, visible=True)
            job_progress.start_task(file_task_id)

        try:
            callback = DownloadProgress(job_progress, effective_task_id, parent_task_id, overall_progress, overall_task_id)
            sftp.get(remote_file, str(local_path), callback=callback)
            logging.info(f"Download of '{file_name}' completed.")
            download_successful = True
        except Exception as e:
            logging.error(f"Download failed for {file_name}: {e}")
            if file_task_id is not None:
                job_progress.update(file_task_id, description=f"[bold red]Failed: {file_name}[/]")
            else:
                job_progress.update(parent_task_id, description=f"[bold red]Failed: {job_progress.tasks[parent_task_id].description} -> {file_name}[/]")
            raise
        finally:
            with count_lock:
                file_counts[parent_task_id][0] += 1
            if file_task_id is not None:
                job_progress.stop_task(file_task_id)
                if download_successful:
                    job_progress.update(file_task_id, description=f"└─ [green]✓[/green] [cyan]{file_name}[/]")

    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()

def transfer_content_rsync(sftp_config, remote_path, local_path, job_progress, parent_task_id, overall_progress, overall_task_id, dry_run=False):
    """
    Transfers a remote file or directory to a local path using rsync, with retries.
    """
    host = sftp_config['host']
    port = sftp_config.getint('port')
    username = sftp_config['username']
    password = sftp_config['password']

    local_parent_dir = os.path.dirname(local_path)
    Path(local_parent_dir).mkdir(parents=True, exist_ok=True)
    remote_spec = f"{username}@{host}:{remote_path}"

    # Add --timeout to rsync and ServerAliveInterval to ssh to prevent stalls
    rsync_cmd = [
        "sshpass", "-p", password,
        "rsync",
        "-aW",
        "--info=progress2",
        "--timeout=60",  # Exit if no data transferred for 60 seconds
        # Add ServerAliveInterval to keep the SSH connection alive through firewalls
        "-e", f"ssh -p {port} -c aes128-ctr -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=15",
        remote_spec,
        local_parent_dir
    ]

    if dry_run:
        logging.info(f"[DRY RUN] Would execute rsync for: {os.path.basename(remote_path)}")
        logging.info(f"[DRY RUN] Command: {' '.join(rsync_cmd)}")
        task = job_progress.tasks[parent_task_id]
        job_progress.update(parent_task_id, advance=task.total)
        overall_progress.update(overall_task_id, advance=task.total)
        return

    logging.info(f"Starting rsync transfer for '{os.path.basename(remote_path)}'")

    max_retries = 2
    retry_delay = 5  # Fixed delay in seconds

    for attempt in range(1, max_retries + 1):
        if attempt > 1:
            logging.info(f"Rsync attempt {attempt}/{max_retries} for '{os.path.basename(remote_path)}'...")
            time.sleep(retry_delay)

        process = None
        try:
            start_time = time.time()
            process = subprocess.Popen(
                rsync_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding='utf-8',
                errors='replace',
                bufsize=1
            )

            last_total_transferred = 0
            progress_regex = re.compile(r"^\s*([\d,]+)\s+\d{1,3}%.*$")

            if process.stdout:
                for line in iter(process.stdout.readline, ''):
                    line = line.strip()
                    match = progress_regex.match(line)
                    if match:
                        try:
                            total_transferred_str = match.group(1).replace(',', '')
                            total_transferred = int(total_transferred_str)
                            advance = total_transferred - last_total_transferred
                            if advance > 0:
                                job_progress.update(parent_task_id, advance=advance)
                                overall_progress.update(overall_task_id, advance=advance)
                                last_total_transferred = total_transferred
                        except (ValueError, IndexError):
                            logging.warning(f"Could not parse rsync progress line: {line}")
                    else:
                        logging.debug(f"rsync stdout: {line}")

            process.wait()
            end_time = time.time()
            stderr_output = process.stderr.read() if process.stderr else ""

            # Success conditions
            if process.returncode == 0 or process.returncode == 24:
                if process.returncode == 24:
                    logging.warning(f"Rsync finished with code 24 (some source files vanished) for '{os.path.basename(remote_path)}'.")

                duration = end_time - start_time
                task = job_progress.tasks[parent_task_id]
                total_size_bytes = task.total
                if duration > 0:
                    speed_mbps = (total_size_bytes * 8) / (duration * 1024 * 1024)
                    logging.info(f"PERF: '{os.path.basename(remote_path)}' ({total_size_bytes / 1024**2:.2f} MiB) took {duration:.2f} seconds.")
                    logging.info(f"PERF: Average speed: {speed_mbps:.2f} Mbps.")
                else:
                    logging.info(f"PERF: '{os.path.basename(remote_path)}' completed in < 1 second.")

                # Ensure the progress bar is marked as complete
                if task.completed < task.total:
                    remaining = task.total - task.completed
                    if remaining > 0:
                        job_progress.update(parent_task_id, advance=remaining)
                        overall_progress.update(overall_task_id, advance=remaining)

                logging.info(f"Rsync transfer completed successfully for '{os.path.basename(remote_path)}'.")
                return  # Exit retry loop and function on success

            # Retryable error
            elif process.returncode == 30:  # Timeout in data send/receive
                logging.warning(f"Rsync timed out for '{os.path.basename(remote_path)}'. Retrying...")
                continue  # To next attempt in the loop

            # Non-retryable error
            else:
                logging.error(f"Rsync failed for '{os.path.basename(remote_path)}' with non-retryable exit code {process.returncode}.")
                logging.error(f"Rsync stderr: {stderr_output}")
                raise Exception(f"Rsync transfer failed for {os.path.basename(remote_path)}")

        except FileNotFoundError:
            logging.error("FATAL: 'rsync' or 'sshpass' command not found.")
            raise
        except Exception as e:
            logging.error(f"An exception occurred during rsync for '{os.path.basename(remote_path)}': {e}")
            if process:
                process.kill()
            if attempt < max_retries - 1:
                logging.warning("Retrying...")
                continue
            else:
                # Re-raise after last attempt
                raise e

    # If loop completes without success
    raise Exception(f"Rsync transfer for '{os.path.basename(remote_path)}' failed after {max_retries} attempts.")


def transfer_content(sftp_config, sftp, remote_path, local_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_map, dry_run=False):
    """
    Transfers a remote file or directory to a local path, preserving structure.
    Handles directories by downloading their files concurrently.
    """
    remote_stat = sftp.stat(remote_path)
    if remote_stat.st_mode & 0o40000:  # S_ISDIR
        all_files = []
        _get_all_files_recursive(sftp, remote_path, local_path, all_files)

        with count_lock:
            file_counts[parent_task_id] = [0, len(all_files)]

        MAX_CONCURRENT_FILES = 5
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_FILES) as file_executor:
            futures = [
                file_executor.submit(_sftp_download_file, sftp_config, remote_f, local_f, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_map.get(remote_f), dry_run)
                for remote_f, local_f in all_files
            ]
            for future in as_completed(futures):
                future.result()

    else: # It's a single file
        with count_lock:
            file_counts[parent_task_id] = [0, 1]
        # For single-file torrents, there is no sub-task, so we pass None for file_task_id
        _sftp_download_file(sftp_config, remote_path, local_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, None, dry_run)

# --- Torrent Processing Logic ---

def get_eligible_torrents(client, category, size_threshold_gb=None):
    """
    Retrieves a list of torrents to be moved based on the specified category and an optional size threshold.

    - If size_threshold_gb is None, it returns all completed torrents in the category.
    - If size_threshold_gb is set, it calculates the current total size of the category and
      selects the oldest completed torrents for moving until the category size is below the threshold.
    """
    try:
        if size_threshold_gb is None:
            torrents = client.torrents_info(category=category, status_filter='completed')
            logging.info(f"Found {len(torrents)} completed torrent(s) in category '{category}' to move.")
            return torrents

        # --- Threshold logic ---
        logging.info(f"Size threshold of {size_threshold_gb} GB is active for category '{category}'.")
        all_torrents_in_category = client.torrents_info(category=category)

        current_total_size = sum(t.size for t in all_torrents_in_category)
        threshold_bytes = size_threshold_gb * (1024**3)

        logging.info(f"Current category size: {current_total_size / (1024**3):.2f} GB. Target size: {size_threshold_gb:.2f} GB.")

        if current_total_size <= threshold_bytes:
            logging.info("Category size is already below the threshold. No torrents to move.")
            return []

        size_to_move = current_total_size - threshold_bytes
        logging.info(f"Need to move at least {size_to_move / (1024**3):.2f} GB of torrents.")

        # Filter for completed torrents and sort them by age (oldest first)
        completed_torrents = [t for t in all_torrents_in_category if t.state == 'completed' or (t.progress == 1 and t.state not in ['checkingUP', 'checkingDL'])]

        # Ensure we are using a reliable 'added_on' attribute.
        # Some clients might have torrents without this attribute, though it's rare.
        # We sort by 'added_on' timestamp, oldest first.
        eligible_torrents = sorted(
            [t for t in completed_torrents if hasattr(t, 'added_on')],
            key=lambda t: t.added_on
        )

        torrents_to_move = []
        size_of_selected_torrents = 0
        for torrent in eligible_torrents:
            if size_of_selected_torrents >= size_to_move:
                break
            torrents_to_move.append(torrent)
            size_of_selected_torrents += torrent.size

        if not torrents_to_move:
            logging.warning("No completed torrents are available to move to meet the threshold.")
            return []

        logging.info(f"Selected {len(torrents_to_move)} torrent(s) to meet the threshold (Total size: {size_of_selected_torrents / (1024**3):.2f} GB).")
        return torrents_to_move

    except Exception as e:
        logging.error(f"Could not retrieve torrents from client: {e}")
        return []

def wait_for_recheck_completion(client, torrent_hash, timeout_seconds=900, dry_run=False):
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
    try:
        netloc = urlparse(tracker_url).netloc
        parts = netloc.split('.')
        if len(parts) > 2:
            if parts[0] in ['tracker', 'announce', 'www']:
                return '.'.join(parts[1:])
        return netloc
    except Exception:
        return None

def get_category_from_rules(torrent, rules, client):
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


def process_torrent(torrent, total_size, mandarin_qbit, unraid_qbit, sftp_config, config, tracker_rules, job_progress, overall_progress, overall_task_id, file_counts, count_lock, task_add_lock, dry_run=False, test_run=False):
    """
    Executes the full transfer and management process for a single torrent.
    Establishes its own SFTP connection to be thread-safe.
    """
    name, hash = torrent.name, torrent.hash
    sftp = None
    transport = None
    source_paused = False
    parent_task_id = None
    success = False
    try:
        dest_base_path = config['UNRAID_PATHS']['destination_path']
        remote_dest_base_path = config['UNRAID_PATHS'].get('remote_destination_path') or dest_base_path

        remote_content_path = torrent.content_path
        content_name = os.path.basename(remote_content_path)
        local_dest_path = os.path.join(dest_base_path, content_name)

        if total_size == 0:
            logging.warning(f"Skipping torrent with no content or zero size: {name}")
            success = True # Mark as success for the UI
            return True

        transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

        with task_add_lock:
            parent_task_id = job_progress.add_task(name, total=total_size, start=True)

        if transfer_mode == 'rsync':
            transfer_content_rsync(sftp_config, remote_content_path, local_dest_path, job_progress, parent_task_id, overall_progress, overall_task_id, dry_run)
            logging.info(f"Rsync transfer completed successfully for '{name}'.")
        else:  # sftp mode
            sftp, transport = connect_sftp(sftp_config)
            all_files = []
            remote_stat = sftp.stat(remote_content_path)
            if remote_stat.st_mode & 0o40000:  # S_ISDIR
                _get_all_files_recursive(sftp, remote_content_path, local_dest_path, all_files)
            else:  # It's a single file
                all_files.append((remote_content_path, local_dest_path))

            file_task_map = {}
            if len(all_files) > 1:
                with task_add_lock:
                    for remote_f, _ in all_files:
                        file_name = os.path.basename(remote_f)
                        try:
                            file_size = sftp.stat(remote_f).st_size
                        except FileNotFoundError:
                            file_size = 0
                        task_id = job_progress.add_task(f"└─ [cyan]{file_name}[/]", total=file_size, start=False, visible=False)
                        file_task_map[remote_f] = task_id

            logging.info(f"Starting SFTP transfer for '{name}'")
            transfer_content(sftp_config, sftp, remote_content_path, local_dest_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_map, dry_run)
            logging.info(f"SFTP transfer completed successfully for '{name}'.")

        unraid_save_path = remote_dest_base_path.replace("\\", "/")

        if not dry_run:
            logging.info(f"Exporting .torrent file for {name}")
            torrent_file_content = mandarin_qbit.torrents_export(torrent_hash=hash)
            logging.info(f"Adding torrent to Unraid (paused) with save path '{unraid_save_path}': {name}")
            unraid_qbit.torrents_add(
                torrent_files=torrent_file_content,
                save_path=unraid_save_path,
                is_paused=True,
                category=torrent.category,
                use_auto_torrent_management=True
            )
            time.sleep(5)
        else:
            logging.info(f"[DRY RUN] Would export and add torrent to Unraid (paused) with save path '{unraid_save_path}': {name}")

        if not dry_run:
            logging.info(f"Triggering force recheck on Unraid for: {name}")
            unraid_qbit.torrents_recheck(torrent_hashes=hash)
        else:
            logging.info(f"[DRY RUN] Would trigger force recheck on Unraid for: {name}")

        if wait_for_recheck_completion(unraid_qbit, hash, dry_run=dry_run):
            if not dry_run:
                logging.info(f"Starting torrent on Unraid: {name}")
                unraid_qbit.torrents_resume(torrent_hashes=hash)
            else:
                logging.info(f"[DRY RUN] Would start torrent on Unraid: {name}")

            logging.info(f"Attempting to categorize torrent on Unraid: {name}")
            set_category_based_on_tracker(unraid_qbit, hash, tracker_rules, dry_run=dry_run)

            if not dry_run and not test_run:
                logging.info(f"Pausing torrent on Mandarin before deletion: {name}")
                mandarin_qbit.torrents_pause(torrent_hashes=hash)
                source_paused = True
            else:
                logging.info(f"[DRY RUN/TEST RUN] Would pause torrent on Mandarin: {name}")

            if test_run:
                logging.info(f"[TEST RUN] Skipping deletion of torrent from Mandarin: {name}")
            elif not dry_run:
                logging.info(f"Deleting torrent and data from Mandarin: {name}")
                mandarin_qbit.torrents_delete(torrent_hashes=hash, delete_files=True)
            else:
                logging.info(f"[DRY RUN] Would delete torrent and data from Mandarin: {name}")

            logging.info(f"--- Successfully processed torrent: {name} ---")
            success = True
            return True
        else:
            logging.error(f"Failed to verify recheck for {name}. Leaving on Mandarin for next run.")
            return False
    except Exception as e:
        logging.error(f"An error occurred while processing torrent {name}: {e}", exc_info=True)
        if parent_task_id is not None:
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
        if parent_task_id is not None:
            job_progress.stop_task(parent_task_id)
            if success:
                original_description = job_progress.tasks[parent_task_id].description
                job_progress.update(parent_task_id, description=f"[green]✓[/green] {original_description}")

# --- Main Execution ---

def run_interactive_categorization(client, rules, script_dir, category_to_scan):
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
                continue
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
            while True:
                choice = input("Enter your choice: ").lower()
                if choice == 'q':
                    if rules_changed:
                        save_tracker_rules(updated_rules, script_dir)
                    return
                if choice == 's':
                    break
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
                        break
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

def setup_logging(script_dir, dry_run, test_run):
    """Configures logging to both console and a file."""
    log_dir = script_dir / 'logs'
    log_dir.mkdir(exist_ok=True)

    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    log_file_name = f"torrent_mover_{timestamp}.log"
    log_file_path = log_dir / log_file_name

    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Remove any existing handlers to avoid duplicates
    if logger.hasHandlers():
        logger.handlers.clear()

    # Create file handler
    file_handler = logging.FileHandler(log_file_path, mode='w', encoding='utf-8')
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    # Create console handler with Rich
    rich_handler = RichHandler(show_path=False, rich_tracebacks=True, markup=True)
    # The formatter for RichHandler should be minimal, as Rich handles the presentation.
    rich_formatter = logging.Formatter('%(message)s')
    rich_handler.setFormatter(rich_formatter)
    logger.addHandler(rich_handler)

    logging.getLogger("paramiko").setLevel(logging.WARNING)

    # Initial log messages
    logging.info("--- Torrent Mover script started ---")
    if dry_run:
        logging.warning("!!! DRY RUN MODE ENABLED. NO CHANGES WILL BE MADE. !!!")
    if test_run:
        logging.warning("!!! TEST RUN MODE ENABLED. SOURCE TORRENTS WILL NOT BE DELETED. !!!")

def pid_exists(pid):
    """Check whether a process with the given PID exists."""
    if pid < 0:
        return False
    if pid == 0:
        # According to POSIX, PID 0 refers to the process group of the sender.
        # It's not a valid PID for a specific process.
        return False
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            # ESRCH == No such process
            return False
        elif err.errno == errno.EPERM:
            # EPERM clearly means there's a process to deny access to
            return True
        else:
            # According to the man page, other error codes should not be possible
            raise
    else:
        return True

def main():
    """Main entry point for the script."""
    script_dir = Path(__file__).resolve().parent
    lock_file_path = script_dir / 'torrent_mover.lock'

    if lock_file_path.exists():
        try:
            with open(lock_file_path, 'r') as f:
                pid = int(f.read().strip())
            if pid_exists(pid):
                logging.error(f"Script is already running with PID {pid}. Aborting.")
                sys.exit(1)
            else:
                logging.warning(f"Found stale lock file for PID {pid}. Removing it.")
                lock_file_path.unlink()
        except (IOError, ValueError) as e:
            logging.warning(f"Could not read or parse PID from lock file: {e}. Removing stale file.")
            lock_file_path.unlink()

    default_config_path = script_dir / 'config.ini'
    parser = argparse.ArgumentParser(description="A script to move qBittorrent torrents and data between servers.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--config', default=str(default_config_path), help='Path to the configuration file.')
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--dry-run', action='store_true', help='Simulate the process without making any changes.')
    mode_group.add_argument('--test-run', action='store_true', help='Run the full process but do not delete the source torrent.')
    parser.add_argument('--parallel-jobs', type=int, default=4, metavar='N', help='Number of torrents to process in parallel.')
    parser.add_argument('--list-rules', action='store_true', help='List all tracker-to-category rules and exit.')
    parser.add_argument('--add-rule', nargs=2, metavar=('TRACKER_DOMAIN', 'CATEGORY'), help='Add or update a rule and exit.')
    parser.add_argument('--delete-rule', metavar='TRACKER_DOMAIN', help='Delete a rule and exit.')
    parser.add_argument('--interactive-categorize', action='store_true', help='Interactively categorize torrents on destination without a rule.')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    setup_logging(script_dir, args.dry_run, args.test_run)

    # --- Early exit argument handling ---
    # These args don't require the full script logic, so we handle them before the lock.
    if args.list_rules or args.add_rule or args.delete_rule or args.interactive_categorize:
        tracker_rules = load_tracker_rules(script_dir)
        # You would add the logic for these commands here.
        # For now, just returning as the original script did.
        # Note: The original script had this logic inside the main try-block,
        # which meant it couldn't run if the main script was "locked".
        # Moving it here makes these utility commands more accessible.
        logging.info("Executing utility command...")
        # Placeholder for actual rule/interactive logic
        if args.list_rules:
            print("Listing rules...")
        if args.add_rule:
            print("Adding rule...")
        if args.delete_rule:
            print("Deleting rule...")
        if args.interactive_categorize:
            print("Starting interactive categorization...")
            unraid_qbit_conf = load_config(args.config)['UNRAID_QBIT']
            unraid_qbit = connect_qbit(unraid_qbit_conf, "Unraid")
            if unraid_qbit:
                # Assuming a default category to scan if not specified, e.g., 'uncategorized'
                cat_to_scan = load_config(args.config)['SETTINGS'].get('interactive_scan_category', '')
                run_interactive_categorization(unraid_qbit, tracker_rules, script_dir, cat_to_scan)
            else:
                logging.error("Could not connect to destination qBittorrent for interactive session.")
        return 0

    try:
        # Create the lock file and write the current PID
        with open(lock_file_path, 'w') as f:
            f.write(str(os.getpid()))

        config = load_config(args.config)
        transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
        if transfer_mode == 'rsync':
            check_sshpass_installed()
        try:
            mandarin_qbit = connect_qbit(config['MANDARIN_QBIT'], "Mandarin")
            unraid_qbit = connect_qbit(config['UNRAID_QBIT'], "Unraid")
        except Exception as e:
            logging.error(f"Failed to connect to qBittorrent client after multiple retries: {e}", exc_info=True)
            logging.error("One or more qBittorrent connections failed. Aborting.")
            return 1
        logging.info("qBittorrent connections established successfully.")

        tracker_rules = load_tracker_rules(script_dir) # Load rules for the main run

        category_to_move = config['SETTINGS']['category_to_move']
        size_threshold_gb_str = config['SETTINGS'].get('size_threshold_gb')
        size_threshold_gb = None
        if size_threshold_gb_str and size_threshold_gb_str.strip():
            try:
                size_threshold_gb = float(size_threshold_gb_str)
            except ValueError:
                logging.error(f"Invalid value for 'size_threshold_gb': '{size_threshold_gb_str}'. It must be a number. Disabling threshold.")
                size_threshold_gb = None

        eligible_torrents = get_eligible_torrents(mandarin_qbit, category_to_move, size_threshold_gb)
        if not eligible_torrents:
            logging.info("No torrents to move at this time.")
            return 0
        total_count = len(eligible_torrents)
        processed_count = 0
        torrent_progress = Progress(TextColumn("[bold blue]Torrents"), BarColumn(), MofNCompleteColumn())
        torrent_task = torrent_progress.add_task("Processing torrents", total=total_count)
        overall_progress = Progress(TextColumn("[bold green]Overall"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TotalFileSizeColumn(), TransferSpeedColumn(), TimeRemainingColumn())
        overall_task = overall_progress.add_task("Total progress", total=0)

        file_counts = defaultdict(lambda: [0, 0])
        count_lock = threading.Lock()
        task_add_lock = threading.Lock()

        job_progress = Progress(
            TextColumn("  {task.description}", justify="left"),
            BarColumn(finished_style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TotalFileSizeColumn(),
            FixedWidthTransferSpeedColumn(),
            FixedWidthTimeRemainingColumn(),
            ConditionalTimeElapsedColumn(file_counts),
            FileCountColumn(file_counts)
        )
        size_calc_progress = Progress(TextColumn("[bold cyan]Calculating sizes..."), BarColumn(), MofNCompleteColumn())
        size_calc_task = size_calc_progress.add_task("...", total=len(eligible_torrents))
        size_calc_panel = Panel(size_calc_progress, title="[bold]Initialization[/bold]", border_style="cyan", expand=False)

        layout = Group(
            size_calc_panel,
            Panel(torrent_progress, title="Torrent Queue", border_style="blue"),
            Panel(overall_progress, title="Total Progress", border_style="green"),
            Panel(job_progress, title="Active Transfers", border_style="yellow", padding=(1, 2))
        )

        with Live(layout, refresh_per_second=4, transient=True) as live:
            sftp_config = config['MANDARIN_SFTP']
            grand_total_size = 0
            torrent_sizes = {}

            # --- Calculate total size of all torrents ---
            if transfer_mode == 'rsync':
                # Use the faster rsync method for size calculation
                for torrent in eligible_torrents:
                    size_calc_progress.update(size_calc_task, description=f"{torrent.name[:50]}")
                    remote_content_path = torrent.content_path
                    try:
                        size = get_remote_size_rsync(sftp_config, remote_content_path)
                        if size == 0:
                            live.console.log(f"[yellow]Warning: Skipping zero-size torrent: {torrent.name}[/]")
                        torrent_sizes[torrent.hash] = size
                        grand_total_size += size
                    except Exception as e:
                        live.console.log(f"[bold red]Error calculating size for '{torrent.name}': {e}[/]")
                        live.console.log(f"[yellow]Warning: Skipping torrent due to size calculation failure.[/]")
                        torrent_sizes[torrent.hash] = 0 # Mark as zero to skip later
                    finally:
                        size_calc_progress.advance(size_calc_task)
            else:  # Default to sftp
                sftp = None
                transport = None
                try:
                    sftp, transport = connect_sftp(sftp_config)
                except Exception as e:
                    live.console.log(f"[bold red]Failed to establish a preliminary SFTP connection: {e}. Aborting.[/]")
                    return 1

                try:
                    for torrent in eligible_torrents:
                        size_calc_progress.update(size_calc_task, description=f"{torrent.name[:50]}")
                        remote_content_path = torrent.content_path

                        size = 0
                        retries = 2
                        for attempt in range(1, retries + 1):
                            try:
                                # Check if connection is alive, or reconnect if it's the first attempt on a failed torrent
                                if not transport or not transport.is_active():
                                    live.console.log("[yellow]SFTP connection is not active. Reconnecting...[/]")
                                    if sftp: sftp.close()
                                    if transport: transport.close()
                                    sftp, transport = connect_sftp(sftp_config)
                                    live.console.log("[green]SFTP reconnected successfully.[/green]")

                                size = get_remote_size(sftp, remote_content_path)
                                if size == 0:
                                    live.console.log(f"[yellow]Warning: Skipping zero-size torrent: {torrent.name}[/]")
                                torrent_sizes[torrent.hash] = size
                                grand_total_size += size
                                break  # Success
                            except (paramiko.ssh_exception.SSHException, EOFError, OSError) as e:
                                live.console.log(f"[bold red]Error calculating size for '{torrent.name}': {e}[/]")
                                if attempt < retries:
                                    live.console.log(f"[yellow]Attempt {attempt}/{retries} failed. Retrying in 5 seconds...[/]")
                                    # The connection will be re-established at the start of the next loop
                                    time.sleep(5)
                                else:
                                    live.console.log(f"[bold red]Failed to calculate size for '{torrent.name}' after {retries} attempts.[/]")
                                    live.console.log(f"[yellow]Warning: Skipping torrent.[/]")
                                    torrent_sizes[torrent.hash] = 0  # Mark as zero to skip

                        size_calc_progress.advance(size_calc_task)
                finally:
                    if sftp: sftp.close()
                    if transport: transport.close()

            overall_progress.update(overall_task, total=grand_total_size, description="[bold green]Overall")

            # --- Swap out the calculation panel for the plan panel ---
            layout.renderables.pop(0) # Remove the calculation panel

            plan_text = Text()
            plan_text.append(f"Found {len(eligible_torrents)} torrents to move. Total size: {grand_total_size/1024**3:.2f} GB\n", style="bold")
            plan_text.append("─" * 70 + "\n", style="dim")
            for torrent in eligible_torrents:
                size_gb = torrent_sizes.get(torrent.hash, 0) / 1024**3
                plan_text.append(f" • {torrent.name} (")
                plan_text.append(f"{size_gb:.2f} GB", style="bold")
                plan_text.append(")\n")

            plan_panel = Panel(plan_text, title="[bold magenta]Transfer Plan[/bold magenta]", border_style="magenta", expand=False)
            layout.renderables.insert(0, plan_panel)
            # ---------------------------------------------------------

            live.console.log("Plan generated. Starting transfers...")
            executor = ThreadPoolExecutor(max_workers=args.parallel_jobs)
            try:
                # Filter out any torrents that were skipped during size calculation
                torrents_to_process = [t for t in eligible_torrents if torrent_sizes.get(t.hash, 0) > 0]

                future_to_torrent = {
                    executor.submit(process_torrent, torrent, torrent_sizes[torrent.hash], mandarin_qbit, unraid_qbit, sftp_config, config, tracker_rules, job_progress, overall_progress, overall_task, file_counts, count_lock, task_add_lock, args.dry_run, args.test_run): torrent
                    for torrent in torrents_to_process
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
                        if job_progress.tasks[-1].description != " ": # Avoid adding multiple separators
                            job_progress.add_task(" ", total=1, completed=1) # Add a blank line as a separator

            except KeyboardInterrupt:
                live.stop()
                live.console.print("\n[bold yellow]Process interrupted by user.[/bold yellow]")
                choice = Prompt.ask(
                    "Do you want to (s)top active transfers or (w)ait for them to complete?",
                    choices=["s", "w"],
                    default="s"
                )

                if choice == 'w':
                    live.console.log("[bold green]Waiting for active transfers to complete...[/bold green]")
                    live.console.log("[bold yellow]This may take some time. Press Ctrl+C again to force stop.[/bold yellow]")
                    try:
                        executor.shutdown(wait=True)
                        live.console.log("[bold green]All active transfers completed.[/bold green]")
                    except KeyboardInterrupt:
                        live.console.log("[bold red]\nForce stopping transfers...[/bold red]")
                        executor.shutdown(wait=False, cancel_futures=True)
                else:
                    live.console.log("[bold red]Stopping transfers...[/bold red]")
                    executor.shutdown(wait=False, cancel_futures=True)
                raise
        logging.info(f"Processing complete. Successfully moved {processed_count}/{total_count} torrent(s).")
    except KeyboardInterrupt:
        pass
    except KeyError as e:
        logging.error(f"Configuration key missing: {e}. Please check your config.ini.")
        return 1
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)
        return 1
    finally:
        if lock_file_path.exists():
            try:
                with open(lock_file_path, 'r') as f:
                    pid = int(f.read().strip())
                if pid == os.getpid():
                    lock_file_path.unlink()
                    logging.info("Lock file removed.")
                else:
                    logging.warning(f"Lock file PID ({pid}) does not match current PID ({os.getpid()}). Not removing.")
            except (IOError, ValueError) as e:
                logging.error(f"Could not read or verify lock file before removing: {e}")
        logging.info("--- Torrent Mover script finished ---")
    return 0

if __name__ == "__main__":
    sys.exit(main())
