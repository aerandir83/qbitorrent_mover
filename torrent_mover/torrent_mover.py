#!/usr/bin/env python3
# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

__version__ = "1.3.1"

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
import tempfile
import getpass


SSH_CONTROL_PATH = None

def setup_ssh_control_path():
    """Creates a directory for the SSH control socket."""
    global SSH_CONTROL_PATH
    try:
        # Create a user-specific temporary directory for the control socket
        user = getpass.getuser()
        control_dir = Path(tempfile.gettempdir()) / f"torrent_mover_ssh_{user}"
        control_dir.mkdir(mode=0o700, exist_ok=True)
        SSH_CONTROL_PATH = str(control_dir / "%r@%h:%p")
        logging.info(f"Using SSH control path: {SSH_CONTROL_PATH}")
    except Exception as e:
        logging.warning(f"Could not create SSH control path directory. Multiplexing will be disabled. Error: {e}")
        SSH_CONTROL_PATH = None


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
    # Setup the directory for SSH connection multiplexing
    setup_ssh_control_path()


def _get_ssh_command(port):
    """Builds the SSH command for rsync, enabling connection multiplexing if available."""
    base_ssh_cmd = f"ssh -p {port} -c chacha20-poly1305@openssh.com -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=15"
    if SSH_CONTROL_PATH:
        multiplex_opts = f"-o ControlMaster=auto -o ControlPath={SSH_CONTROL_PATH} -o ControlPersist=60s"
        return f"{base_ssh_cmd} {multiplex_opts}"
    return base_ssh_cmd


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
    Connects to a server via SFTP using SSHClient for better timeout control.
    Returns a connected SFTP client and the SSHClient object, or raises an exception on failure.
    """
    host = config_section['host']
    port = config_section.getint('port')
    username = config_section['username']
    password = config_section['password']

    logging.info(f"Establishing SFTP connection to {host}...")
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # The timeout parameter here is crucial for fast failure detection
    ssh_client.connect(hostname=host, port=port, username=username, password=password, timeout=10)

    # Enable keepalives on the underlying transport
    transport = ssh_client.get_transport()
    if transport:
        transport.set_keepalive(30)

    sftp = ssh_client.open_sftp()
    logging.info(f"Successfully established SFTP connection to {host}.")
    return sftp, ssh_client

import os
import sys
import time
import argparse
import argcomplete
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
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

# --- File Transfer Logic ---

def get_remote_size_sftp(sftp, remote_path):
    """Recursively gets the total size of a remote file or directory via SFTP."""
    total_size = 0
    try:
        stat = sftp.stat(remote_path)
        if stat.st_mode & 0o40000:  # S_ISDIR
            for item in sftp.listdir(remote_path):
                item_path = f"{remote_path.rstrip('/')}/{item}"
                total_size += get_remote_size_sftp(sftp, item_path)
        else:
            total_size = stat.st_size
    except FileNotFoundError:
        logging.warning(f"Could not stat remote path for size calculation: {remote_path}")
        return 0
    return total_size

@retry(tries=2, delay=5)
def _get_remote_files_rsync(sftp_config, remote_path):
    """
    Gets the list of files and their sizes from a remote directory using `find`.
    Returns a list of (size, path) tuples and the total size.
    """
    host = sftp_config['host']
    port = sftp_config.getint('port')
    username = sftp_config['username']
    password = sftp_config['password']

    # This command securely lists all files in the remote path, printing their size and relative path.
    # It changes to the directory first to ensure the paths are relative.
    # Using single quotes around the shell command executed by ssh is crucial for handling special characters.
    remote_command = f"cd '{remote_path}' && find . -type f -printf '%s %p\\n'"

    ssh_cmd = [
        "sshpass", "-p", password,
        *_get_ssh_command(port).split(),
        f"{username}@{host}",
        remote_command
    ]

    try:
        result = subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            check=True # Raise CalledProcessError on non-zero exit codes
        )
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to list remote files for '{os.path.basename(remote_path)}' via SSH.")
        logging.error(f"Stderr: {e.stderr}")
        raise Exception(f"Failed to list remote files via SSH. Exit code: {e.returncode}.") from e
    except Exception as e:
        logging.error(f"An unexpected error occurred while listing remote files: {e}")
        raise

    file_list = []
    total_size = 0
    # Process the output from the find command
    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        try:
            size_str, file_path_str = line.split(' ', 1)
            size = int(size_str)
            # Remove the leading './' from find's output
            relative_path = file_path_str[2:] if file_path_str.startswith('./') else file_path_str
            file_list.append((size, relative_path))
            total_size += size
        except ValueError:
            logging.warning(f"Could not parse line from remote find output: '{line}'")
            continue

    return file_list, total_size


def _get_all_files_recursive_sftp(sftp, remote_path, local_path, file_list):
    """
    Recursively walks a remote directory to build a flat list of all files to download.
    """
    for item in sftp.listdir(remote_path):
        remote_item_path = f"{remote_path.rstrip('/')}/{item}"
        local_item_path = os.path.join(local_path, item)

        stat_info = sftp.stat(remote_item_path)
        if stat_info.st_mode & 0o40000:  # S_ISDIR
            _get_all_files_recursive_sftp(sftp, remote_item_path, local_item_path, file_list)
        else:
            file_list.append((remote_item_path, local_item_path))

@retry(tries=2, delay=5)
def _sftp_download_file(sftp_config, remote_file, local_file, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_id, dry_run=False):
    """
    Downloads a single file with a progress bar, with retries. Establishes its own SFTP session
    to ensure thread safety when called from a ThreadPoolExecutor.
    This version supports resuming partial downloads.
    """
    local_path = Path(local_file)
    file_name = os.path.basename(remote_file)

    sftp = None
    ssh_client = None
    download_successful = False
    try:
        sftp, ssh_client = connect_sftp(sftp_config)

        remote_stat = sftp.stat(remote_file)
        total_size = remote_stat.st_size
        logging.debug(f"SFTP Check: Remote file '{remote_file}' size: {total_size}")

        if total_size == 0:
            logging.warning(f"Skipping zero-byte file: {file_name}")
            with count_lock:
                file_counts[parent_task_id][0] += 1
            return

        local_size = 0
        if local_path.exists():
            local_size = local_path.stat().st_size
            logging.debug(f"SFTP Check: Local file '{local_file}' exists with size: {local_size}")
            if local_size == total_size:
                logging.info(f"Skipping (exists and size matches): {file_name}")
                job_progress.update(parent_task_id, advance=total_size)
                overall_progress.update(overall_task_id, advance=total_size)
                if file_task_id is not None:
                    job_progress.update(file_task_id, completed=total_size)
                with count_lock:
                    file_counts[parent_task_id][0] += 1
                return
            elif local_size > total_size:
                logging.warning(f"Local file '{file_name}' is larger than remote ({local_size} > {total_size}), re-downloading from scratch.")
                local_size = 0
            else:
                logging.info(f"Resuming download for {file_name} from {local_size / (1024*1024):.2f} MB.")
        else:
            logging.debug(f"SFTP NEW: Local file '{local_file}' does not exist. Starting new download.")

        effective_task_id = file_task_id if file_task_id is not None else parent_task_id
        job_progress.update(effective_task_id, completed=local_size)

        if local_size > 0:
            if effective_task_id != parent_task_id:
                job_progress.update(parent_task_id, advance=local_size)
            overall_progress.update(overall_task_id, advance=local_size)

        if dry_run:
            logging.info(f"[DRY RUN] Would download: {remote_file} -> {local_path}")
            remaining_size = total_size - local_size
            job_progress.update(parent_task_id, advance=remaining_size)
            overall_progress.update(overall_task_id, advance=remaining_size)
            with count_lock:
                file_counts[parent_task_id][0] += 1
            return

        local_path.parent.mkdir(parents=True, exist_ok=True)

        if file_task_id is not None:
            job_progress.update(file_task_id, visible=True)
            job_progress.start_task(file_task_id)

        try:
            mode = 'r+b' if local_size > 0 else 'wb'
            with sftp.open(remote_file, 'rb') as remote_f:
                remote_f.seek(local_size)
                remote_f.prefetch()
                with open(local_path, mode) as local_f:
                    if local_size > 0:
                        local_f.seek(local_size)
                    while True:
                        chunk = remote_f.read(32768)
                        if not chunk:
                            break
                        local_f.write(chunk)
                        increment = len(chunk)
                        job_progress.update(effective_task_id, advance=increment)
                        if effective_task_id != parent_task_id:
                            job_progress.update(parent_task_id, advance=increment)
                        overall_progress.update(overall_task_id, advance=increment)

            final_local_size = local_path.stat().st_size
            if final_local_size == total_size:
                logging.info(f"Download of '{file_name}' completed.")
                download_successful = True
            else:
                raise Exception(f"Final size mismatch for {file_name}. Expected {total_size}, got {final_local_size}")
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
        if sftp: sftp.close()
        if ssh_client: ssh_client.close()

def _transfer_rsync_chunk_worker(sftp_config, file_chunk, chunk_size, remote_base_path, local_dest, job_progress, parent_task_id, overall_progress, overall_task_id, dry_run=False):
    """
    Worker function to transfer a chunk of files using a single rsync process.
    """
    host = sftp_config['host']
    port = sftp_config.getint('port')
    username = sftp_config['username']
    password = sftp_config['password']

    # Ensure the remote path for rsync has a trailing slash for correct file placement.
    remote_spec = f"{username}@{host}:{remote_base_path.rstrip('/')}/"

    # rsync command to transfer a list of files read from stdin.
    rsync_cmd = [
        "sshpass", "-p", password,
        "rsync",
        "-a", "-W", "--partial", "--inplace",
        "--files-from=-",  # Read file list from stdin
        "--timeout=60",
        "-e", _get_ssh_command(port),
        remote_spec,
        local_dest
    ]

    files_to_transfer_str = "\n".join([item[1] for item in file_chunk])

    if dry_run:
        job_progress.update(parent_task_id, advance=chunk_size)
        overall_progress.update(overall_task_id, advance=chunk_size)
        return

    try:
        # Using Popen to handle stdin and capture output
        process = subprocess.Popen(
            rsync_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL, # Redirect stdout to null to prevent blocking
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8'
        )
        _, stderr = process.communicate(input=files_to_transfer_str)

        if process.returncode != 0:
            raise Exception(f"Rsync worker failed with exit code {process.returncode}. Stderr: {stderr}")

        # On success, advance the progress bars by the total size of the chunk.
        job_progress.update(parent_task_id, advance=chunk_size)
        overall_progress.update(overall_task_id, advance=chunk_size)
        logging.debug(f"Rsync worker successfully transferred a chunk of {len(file_chunk)} files.")

    except Exception as e:
        logging.error(f"An exception occurred in the rsync worker: {e}")
        raise # Re-raise to be handled by the main transfer loop

def transfer_content_rsync(sftp_config, remote_path, local_path, file_list, job_progress, parent_task_id, overall_progress, overall_task_id, rsync_workers, dry_run=False):
    """
    Transfers a remote directory to a local path using multiple parallel rsync workers.
    """
    if not file_list:
        logging.info(f"No files to transfer for torrent '{os.path.basename(remote_path)}'.")
        return

    # Ensure the local destination directory exists.
    Path(local_path).mkdir(parents=True, exist_ok=True)

    def chunk_list(seq, num):
        # Helper to divide a list of items into a specified number of chunks.
        avg = len(seq) / float(num)
        out = []
        last = 0.0
        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg
        return out

    # Divide the file list among the available rsync workers.
    file_chunks = chunk_list(file_list, rsync_workers)

    logging.info(f"Starting rsync transfer for '{os.path.basename(remote_path)}' using {rsync_workers} parallel worker(s).")

    with ThreadPoolExecutor(max_workers=rsync_workers, thread_name_prefix='RsyncWorker') as executor:
        futures = []
        for chunk in file_chunks:
            if not chunk: continue # Skip empty chunks

            # Calculate the total size of files in this chunk for progress reporting.
            chunk_size = sum(item[0] for item in chunk)

            future = executor.submit(
                _transfer_rsync_chunk_worker,
                sftp_config,
                chunk,
                chunk_size,
                remote_path,  # This is the base path for rsync
                local_path,
                job_progress,
                parent_task_id,
                overall_progress,
                overall_task_id,
                dry_run
            )
            futures.append(future)

        # Wait for all futures to complete and raise an exception if any worker failed.
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"A parallel rsync worker failed for torrent '{os.path.basename(remote_path)}'. Aborting transfer for this torrent.")
                # The exception is re-raised to be caught by the main torrent processing loop.
                raise

def transfer_content_sftp(sftp_config, sftp, remote_path, local_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_map, dry_run=False):
    """
    Transfers a remote file or directory to a local path using SFTP, downloading files concurrently.
    """
    remote_stat = sftp.stat(remote_path)
    if remote_stat.st_mode & 0o40000:  # S_ISDIR
        all_files = []
        _get_all_files_recursive_sftp(sftp, remote_path, local_path, all_files)

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
        _sftp_download_file(sftp_config, remote_path, local_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, None, dry_run)

# --- Torrent Processing Logic ---

def get_eligible_torrents(client, category, size_threshold_gb=None):
    """
    Retrieves a list of torrents to be moved based on the specified category and an optional size threshold.
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

        completed_torrents = [t for t in all_torrents_in_category if t.state == 'completed' or (t.progress == 1 and t.state not in ['checkingUP', 'checkingDL'])]
        eligible_torrents = sorted([t for t in completed_torrents if hasattr(t, 'added_on')], key=lambda t: t.added_on)

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


def analyze_torrent(torrent, sftp_config, transfer_mode, live_console):
    """
    Analyzes a single torrent to determine its size and file list.
    Returns a tuple of (torrent, size, file_list). Size is None if calculation fails.
    file_list is specific to rsync mode.
    """
    name = torrent.name
    remote_content_path = torrent.content_path
    logging.info(f"Analyzing torrent: {name}")
    total_size = None
    file_list = None

    try:
        if transfer_mode == 'rsync':
            file_list, total_size = _get_remote_files_rsync(sftp_config, remote_content_path)
        else:  # sftp mode
            sftp, ssh_client = None, None
            try:
                sftp, ssh_client = connect_sftp(sftp_config)
                total_size = get_remote_size_sftp(sftp, remote_content_path)
            finally:
                if sftp: sftp.close()
                if ssh_client: ssh_client.close()
    except Exception as e:
        live_console.log(f"[bold red]Error calculating size for '{name}': {e}[/]")
        live_console.log(f"[yellow]Warning: Skipping torrent due to size calculation failure.[/]")
        return torrent, None, None

    if total_size == 0:
        live_console.log(f"[yellow]Warning: Skipping zero-size torrent: {name}[/]")

    return torrent, total_size, file_list


def transfer_torrent(torrent, total_size, file_list, source_qbit, destination_qbit, sftp_config, config, tracker_rules, job_progress, overall_progress, overall_task_id, file_counts, count_lock, task_add_lock, rsync_workers, dry_run=False, test_run=False):
    """
    Executes the transfer and management process for a single, pre-analyzed torrent.
    """
    name, hash = torrent.name, torrent.hash
    sftp = None
    ssh_client = None
    source_paused = False
    parent_task_id = None
    success = False
    try:
        dest_base_path = config['DESTINATION_PATHS']['destination_path']
        remote_dest_base_path = config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path

        remote_content_path = torrent.content_path
        content_name = os.path.basename(remote_content_path)
        local_dest_path = os.path.join(dest_base_path, content_name)

        transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

        with task_add_lock:
            parent_task_id = job_progress.add_task(name, total=total_size, start=True)

        if transfer_mode == 'rsync':
            transfer_content_rsync(sftp_config, remote_content_path, local_dest_path, file_list, job_progress, parent_task_id, overall_progress, overall_task_id, rsync_workers, dry_run)
            logging.info(f"Rsync transfer completed successfully for '{name}'.")
        else:  # sftp mode
            sftp, ssh_client = connect_sftp(sftp_config)
            all_files = []
            remote_stat = sftp.stat(remote_content_path)
            if remote_stat.st_mode & 0o40000:  # S_ISDIR
                _get_all_files_recursive_sftp(sftp, remote_content_path, local_dest_path, all_files)
            else:
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
            transfer_content_sftp(sftp_config, sftp, remote_content_path, local_dest_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_map, dry_run)
            logging.info(f"SFTP transfer completed successfully for '{name}'.")

        destination_save_path = remote_dest_base_path.replace("\\", "/")

        if not dry_run:
            logging.info(f"Exporting .torrent file for {name}")
            torrent_file_content = source_qbit.torrents_export(torrent_hash=hash)
            logging.info(f"Adding torrent to Destination (paused) with save path '{destination_save_path}': {name}")
            destination_qbit.torrents_add(
                torrent_files=torrent_file_content,
                save_path=destination_save_path,
                is_paused=True,
                category=torrent.category,
                use_auto_torrent_management=True
            )
            time.sleep(5)
        else:
            logging.info(f"[DRY RUN] Would export and add torrent to Destination (paused) with save path '{destination_save_path}': {name}")

        if not dry_run:
            logging.info(f"Triggering force recheck on Destination for: {name}")
            destination_qbit.torrents_recheck(torrent_hashes=hash)
        else:
            logging.info(f"[DRY RUN] Would trigger force recheck on Destination for: {name}")

        if wait_for_recheck_completion(destination_qbit, hash, dry_run=dry_run):
            if not dry_run:
                logging.info(f"Starting torrent on Destination: {name}")
                destination_qbit.torrents_resume(torrent_hashes=hash)
            else:
                logging.info(f"[DRY RUN] Would start torrent on Destination: {name}")

            logging.info(f"Attempting to categorize torrent on Destination: {name}")
            set_category_based_on_tracker(destination_qbit, hash, tracker_rules, dry_run=dry_run)

            if not dry_run and not test_run:
                logging.info(f"Pausing torrent on Source before deletion: {name}")
                source_qbit.torrents_pause(torrent_hashes=hash)
                source_paused = True
            else:
                logging.info(f"[DRY RUN/TEST RUN] Would pause torrent on Source: {name}")

            if test_run:
                logging.info(f"[TEST RUN] Skipping deletion of torrent from Source: {name}")
            elif not dry_run:
                logging.info(f"Deleting torrent and data from Source: {name}")
                source_qbit.torrents_delete(torrent_hashes=hash, delete_files=True)
            else:
                logging.info(f"[DRY RUN] Would delete torrent and data from Source: {name}")

            logging.info(f"--- Successfully processed torrent: {name} ---")
            success = True
            return True
        else:
            logging.error(f"Failed to verify recheck for {name}. Leaving on Source for next run.")
            return False
    except Exception as e:
        logging.error(f"An error occurred while processing torrent {name}: {e}", exc_info=True)
        if parent_task_id is not None:
            job_progress.update(parent_task_id, description=f"[bold red]Failed: {name}[/]")
        if not dry_run and source_paused:
            try:
                source_qbit.torrents_resume(torrent_hashes=hash)
            except Exception as resume_e:
                logging.error(f"Failed to resume torrent {name} on Source after error: {resume_e}")
        return False
    finally:
        if sftp: sftp.close()
        if ssh_client: ssh_client.close()
        if parent_task_id is not None:
            job_progress.stop_task(parent_task_id)
            if success:
                original_description = job_progress.tasks[parent_task_id].description
                job_progress.update(parent_task_id, description=f"[green]✓[/green] {original_description}")

# --- Main Execution ---

def run_interactive_categorization(client, rules, script_dir, category_to_scan, no_rules=False):
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

            print("-" * 60)
            print(f"Torrent needs categorization: [bold]{torrent.name}[/bold]")
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

def setup_logging(script_dir, dry_run, test_run, debug):
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

    rich_handler = RichHandler(show_path=False, rich_tracebacks=True, markup=True)
    rich_formatter = logging.Formatter('%(message)s')
    rich_handler.setFormatter(rich_formatter)
    logger.addHandler(rich_handler)

    logging.getLogger("paramiko").setLevel(logging.WARNING)

    logging.info("--- Torrent Mover script started ---")
    if dry_run:
        logging.warning("!!! DRY RUN MODE ENABLED. NO CHANGES WILL BE MADE. !!!")
    if test_run:
        logging.warning("!!! TEST RUN MODE ENABLED. SOURCE TORRENTS WILL NOT BE DELETED. !!!")

def pid_exists(pid):
    """Check whether a process with the given PID exists."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
        elif err.errno == errno.EPERM:
            return True
        else:
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
    parser.add_argument('--debug', action='store_true', help='Enable debug logging to file.')
    parser.add_argument('--parallel-jobs', type=int, default=4, metavar='N', help='Number of torrents to process in parallel.')
    parser.add_argument('--rsync-workers', type=int, default=4, metavar='N', help='Number of parallel rsync workers per torrent.')
    parser.add_argument('-l', '--list-rules', action='store_true', help='List all tracker-to-category rules and exit.')
    parser.add_argument('-a', '--add-rule', nargs=2, metavar=('TRACKER_DOMAIN', 'CATEGORY'), help='Add or update a rule and exit.')
    parser.add_argument('-d', '--delete-rule', metavar='TRACKER_DOMAIN', help='Delete a rule and exit.')
    parser.add_argument('-c', '--categorize', dest='interactive_categorize', action='store_true', help='Interactively categorize torrents on destination.')
    parser.add_argument('--category', help='(For -c mode) Specify a category to scan, overriding the config.')
    parser.add_argument('-nr', '--no-rules', action='store_true', help='(For -c mode) Ignore existing rules and show all torrents in the category.')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    setup_logging(script_dir, args.dry_run, args.test_run, args.debug)

    if args.list_rules or args.add_rule or args.delete_rule or args.interactive_categorize:
        config = load_config(args.config)
        tracker_rules = load_tracker_rules(script_dir)
        logging.info("Executing utility command...")

        if args.list_rules:
            if not tracker_rules:
                logging.info("No rules found.")
                return 0
            console = Console()
            table = Table(title="Tracker to Category Rules", show_header=True, header_style="bold magenta")
            table.add_column("Tracker Domain", style="dim", width=40)
            table.add_column("Assigned Category")
            sorted_rules = sorted(tracker_rules.items())
            for domain, category in sorted_rules:
                table.add_row(domain, f"[yellow]{category}[/yellow]" if category == "ignore" else f"[cyan]{category}[/cyan]")
            console.print(table)
            return 0

        if args.add_rule:
            domain, category = args.add_rule
            tracker_rules[domain] = category
            if save_tracker_rules(tracker_rules, script_dir):
                logging.info(f"Successfully added rule: '{domain}' -> '{category}'.")
            return 0

        if args.delete_rule:
            domain_to_delete = args.delete_rule
            if domain_to_delete in tracker_rules:
                del tracker_rules[domain_to_delete]
                if save_tracker_rules(tracker_rules, script_dir):
                    logging.info(f"Successfully deleted rule for '{domain_to_delete}'.")
            else:
                logging.warning(f"No rule found for domain '{domain_to_delete}'. Nothing to delete.")
            return 0

        if args.interactive_categorize:
            try:
                dest_client_section = config['SETTINGS'].get('destination_client_section', 'DESTINATION_CLIENT')
                destination_qbit = connect_qbit(config[dest_client_section], "Destination")
                if args.category:
                    cat_to_scan = args.category
                    logging.info(f"Using category from command line: '{cat_to_scan}'")
                else:
                    cat_to_scan = config['SETTINGS'].get('category_to_move', '')
                    if cat_to_scan:
                        logging.info(f"Using 'category_to_move' from config: '{cat_to_scan}'")
                    else:
                        logging.warning("No category specified. Defaulting to 'uncategorized'.")
                run_interactive_categorization(destination_qbit, tracker_rules, script_dir, cat_to_scan, args.no_rules)
            except Exception as e:
                logging.error(f"Failed to run interactive categorization: {e}", exc_info=True)
            return 0
        return 0

    try:
        with open(lock_file_path, 'w') as f:
            f.write(str(os.getpid()))

        config = load_config(args.config)
        transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
        if transfer_mode == 'rsync':
            check_sshpass_installed()

        try:
            source_section_name = config['SETTINGS']['source_client_section']
            dest_section_name = config['SETTINGS']['destination_client_section']
            source_qbit = connect_qbit(config[source_section_name], "Source")
            destination_qbit = connect_qbit(config[dest_section_name], "Destination")
        except KeyError as e:
            logging.error(f"Configuration Error: The client section '{e}' is defined in [SETTINGS] but not found in the config file.")
            logging.error("Please ensure 'source_client_section' and 'destination_client_section' in [SETTINGS] match the actual section names (e.g., [SOURCE_CLIENT]).")
            return 1
        except Exception as e:
            logging.error(f"Failed to connect to qBittorrent client: {e}", exc_info=True)
            return 1
        logging.info("qBittorrent connections established successfully.")

        tracker_rules = load_tracker_rules(script_dir)

        category_to_move = config['SETTINGS']['category_to_move']
        size_threshold_gb_str = config['SETTINGS'].get('size_threshold_gb')
        size_threshold_gb = None
        if size_threshold_gb_str and size_threshold_gb_str.strip():
            try:
                size_threshold_gb = float(size_threshold_gb_str)
            except ValueError:
                logging.error(f"Invalid 'size_threshold_gb': '{size_threshold_gb_str}'. Must be a number. Disabling threshold.")
                size_threshold_gb = None

        eligible_torrents = get_eligible_torrents(source_qbit, category_to_move, size_threshold_gb)
        if not eligible_torrents:
            logging.info("No torrents to move at this time.")
            return 0
        total_count = len(eligible_torrents)
        processed_count = 0

        torrent_progress = Progress(TextColumn("[bold blue]Torrents"), BarColumn(), MofNCompleteColumn())
        analysis_progress = Progress(TextColumn("[bold cyan]Analyzing..."), BarColumn(), MofNCompleteColumn())
        torrent_task = torrent_progress.add_task("Completed", total=total_count)
        analysis_task = analysis_progress.add_task("Analyzed", total=total_count)
        overall_progress = Progress(TextColumn("[bold green]Overall"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TotalFileSizeColumn(), FixedWidthTransferSpeedColumn(), FixedWidthTimeRemainingColumn())
        overall_task = overall_progress.add_task("Total progress", total=0)
        file_counts = defaultdict(lambda: [0, 0])
        count_lock = threading.Lock()
        task_add_lock = threading.Lock()
        plan_lock = threading.Lock()
        overall_progress_lock = threading.Lock()
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
        plan_text = Text(f"Found {total_count} torrents to process...\n", style="bold")
        plan_text.append("─" * 70 + "\n", style="dim")
        plan_panel = Panel(plan_text, title="[bold magenta]Transfer Plan[/bold magenta]", border_style="magenta", expand=False)
        layout = Group(plan_panel, Panel(Group(torrent_progress, analysis_progress), title="Overall Queue", border_style="blue"), Panel(overall_progress, title="Total Progress", border_style="green"), Panel(job_progress, title="Active Transfers", border_style="yellow", padding=(1, 2)))

        with Live(layout, refresh_per_second=4, transient=True) as live:
            sftp_config = config['SOURCE_SERVER']
            transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
            rsync_workers = config['SETTINGS'].getint('rsync_workers', args.rsync_workers)
            analysis_workers = max(10, args.parallel_jobs * 2)

            try:
                with ThreadPoolExecutor(max_workers=analysis_workers, thread_name_prefix='Analyzer') as analysis_executor, \
                     ThreadPoolExecutor(max_workers=args.parallel_jobs, thread_name_prefix='Transfer') as transfer_executor:

                    analysis_future_to_torrent = {
                        analysis_executor.submit(analyze_torrent, torrent, sftp_config, transfer_mode, live.console): torrent
                        for torrent in eligible_torrents
                    }
                    transfer_future_to_torrent = {}

                    for future in as_completed(analysis_future_to_torrent):
                        original_torrent = analysis_future_to_torrent[future]
                        try:
                            analyzed_torrent, total_size, file_list = future.result()
                            analysis_progress.update(analysis_task, advance=1)

                            if total_size is not None and total_size > 0:
                                with plan_lock:
                                    size_gb = total_size / 1024**3
                                    plan_text.append(f" • {analyzed_torrent.name} (")
                                    plan_text.append(f"{size_gb:.2f} GB", style="bold")
                                    plan_text.append(")\n")
                                with overall_progress_lock:
                                    current_total = overall_progress.tasks[overall_task].total
                                    overall_progress.update(overall_task, total=current_total + total_size)

                                transfer_future = transfer_executor.submit(
                                    transfer_torrent, analyzed_torrent, total_size, file_list,
                                    source_qbit, destination_qbit, sftp_config, config, tracker_rules,
                                    job_progress, overall_progress, overall_task,
                                    file_counts, count_lock, task_add_lock,
                                    rsync_workers, args.dry_run, args.test_run
                                )
                                transfer_future_to_torrent[transfer_future] = analyzed_torrent
                            else:
                                torrent_progress.update(torrent_task, advance=1)
                        except Exception as e:
                            live.console.log(f"[bold red]Error during analysis for '{original_torrent.name}': {e}[/]")
                            analysis_progress.update(analysis_task, advance=1)
                            torrent_progress.update(torrent_task, advance=1)

                    live.console.log("[green]All torrents analyzed. Waiting for transfers to complete...[/]")

                    for future in as_completed(transfer_future_to_torrent):
                        torrent = transfer_future_to_torrent[future]
                        try:
                            if future.result():
                                processed_count += 1
                        except Exception as e:
                            live.console.log(f"[bold red]An exception occurred for torrent '{torrent.name}': {e}[/]", exc_info=True)
                        finally:
                            torrent_progress.update(torrent_task, advance=1)
                            if len(job_progress.tasks) > 0 and job_progress.tasks[-1].description != " ":
                                job_progress.add_task(" ", total=1, completed=1)
            except KeyboardInterrupt:
                live.stop()
                live.console.print("\n[bold yellow]Process interrupted by user. Transfers will be cancelled.[/bold yellow]")
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
            except (IOError, ValueError) as e:
                logging.error(f"Could not read or verify lock file before removing: {e}")
        logging.info("--- Torrent Mover script finished ---")
    return 0

if __name__ == "__main__":
    sys.exit(main())