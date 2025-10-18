#!/usr/bin/env python3
# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

__version__ = "1.2.2"

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
from .utils import retry
import tempfile
import getpass
import configupdater


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
    base_ssh_cmd = f"ssh -p {port} -c aes128-ctr -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=15"
    if SSH_CONTROL_PATH:
        multiplex_opts = f"-o ControlMaster=auto -o ControlPath={SSH_CONTROL_PATH} -o ControlPersist=60s"
        return f"{base_ssh_cmd} {multiplex_opts}"
    return base_ssh_cmd


def update_config(config_path, template_path):
    """
    Updates the config.ini from the template, preserving existing values and comments.
    Also creates a backup of the original config file.
    """
    config_file = Path(config_path)
    template_file = Path(template_path)

    if not template_file.is_file():
        logging.error(f"FATAL: Config template '{template_path}' not found.")
        sys.exit(1)

    # If config.ini doesn't exist, create it from the template
    if not config_file.is_file():
        logging.warning(f"Configuration file not found at '{config_path}'.")
        logging.warning("Creating a new one from the template. Please review and fill it out.")
        try:
            shutil.copy2(template_file, config_file)
        except OSError as e:
            logging.error(f"FATAL: Could not create config file: {e}")
            sys.exit(1)
        return

    # --- Update existing config ---
    try:
        # Use ConfigUpdater to preserve comments for both reading and writing
        updater = configupdater.ConfigUpdater()
        updater.read(config_file, encoding='utf-8')

        template_updater = configupdater.ConfigUpdater()
        template_updater.read(template_file, encoding='utf-8')

        changes_made = False
        # Iterate over template sections and options
        for section_name in template_updater.sections():
            template_section = template_updater[section_name]
            if not updater.has_section(section_name):
                # Add the new section with comments
                user_section = updater.add_section(section_name)
                for key, opt in template_section.items():
                    user_opt = user_section.set(key, opt.value)
                    # Check if comments exist before trying to access them
                    if hasattr(opt, 'comments') and opt.comments.above:
                        user_opt.add_comment('\n'.join(opt.comments.above), above=True)
                    if hasattr(opt, 'comments') and opt.comments.inline:
                        user_opt.add_comment(opt.comments.inline, inline=True)
                changes_made = True
                logging.info(f"Added new section to config: [{section_name}]")
            else:
                # Section exists, check for new options
                user_section = updater[section_name]
                for key, opt in template_section.items():
                    if not user_section.has_option(key):
                        user_opt = user_section.set(key, opt.value)
                        # Check if comments exist before trying to access them
                        if hasattr(opt, 'comments') and opt.comments.above:
                            user_opt.add_comment('\n'.join(opt.comments.above), above=True)
                        if hasattr(opt, 'comments') and opt.comments.inline:
                            user_opt.add_comment(opt.comments.inline, inline=True)
                        changes_made = True
                        logging.info(f"Added new option in [{section_name}]: {key}")

        if changes_made:
            # Create a backup in a 'backup' sub-folder
            backup_dir = config_file.parent / 'backup'
            backup_dir.mkdir(exist_ok=True)
            backup_filename = f"{config_file.stem}.bak_{time.strftime('%Y%m%d-%H%M%S')}"
            backup_path = backup_dir / backup_filename
            shutil.copy2(config_file, backup_path)
            logging.info(f"Backed up existing configuration to '{backup_path}'")
            with config_file.open('w', encoding='utf-8') as f:
                updater.write(f)
            logging.info("Configuration file has been updated with new options.")
        else:
            logging.info("Configuration file is already up-to-date.")

    except Exception as e:
        logging.error(f"FATAL: An error occurred during config update: {e}", exc_info=True)
        sys.exit(1)


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

# --- SFTP Transfer Logic with Progress Bar ---

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
        "-e", _get_ssh_command(port),
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


@retry(tries=2, delay=5)
def get_remote_size_du(ssh_client, remote_path):
    """
    Gets the total size of a remote file or directory using 'du -sb'.
    This is significantly faster for directories than recursive SFTP stat calls.
    Requires a connected Paramiko SSHClient.
    """
    # Escape single quotes in the path to prevent command injection issues.
    escaped_path = remote_path.replace("'", "'\\''")
    command = f"du -sb '{escaped_path}'"
    try:
        stdin, stdout, stderr = ssh_client.exec_command(command, timeout=60)
        exit_status = stdout.channel.recv_exit_status()  # Wait for command to finish

        if exit_status != 0:
            stderr_output = stderr.read().decode('utf-8').strip()
            if "No such file or directory" in stderr_output:
                logging.warning(f"'du' command failed for path '{remote_path}': File not found.")
                return 0
            raise Exception(f"'du' command failed with exit code {exit_status}. Stderr: {stderr_output}")

        output = stdout.read().decode('utf-8').strip()
        # The output is like "12345\t/path/to/dir". We only need the number.
        size_str = output.split()[0]
        if size_str.isdigit():
            return int(size_str)
        else:
            raise ValueError(f"Could not parse 'du' output. Raw output: '{output}'")

    except Exception as e:
        logging.error(f"An error occurred executing 'du' command for '{remote_path}': {e}")
        # Re-raise to be handled by the retry decorator or the calling function
        raise


def _get_all_files_recursive(sftp, remote_path, base_dest_path, file_list):
    """
    Recursively walks a remote directory to build a flat list of all files to transfer.
    `base_dest_path` is the corresponding destination path for the initial `remote_path`.
    """
    # Normalize paths to use forward slashes for consistency
    remote_path_norm = remote_path.replace('\\', '/')
    base_dest_path_norm = base_dest_path.replace('\\', '/')

    try:
        items = sftp.listdir(remote_path_norm)
    except FileNotFoundError:
        logging.warning(f"Directory not found on source, skipping: {remote_path_norm}")
        return

    for item in items:
        # Construct full paths, ensuring no double slashes
        remote_item_path = f"{remote_path_norm.rstrip('/')}/{item}"
        dest_item_path = f"{base_dest_path_norm.rstrip('/')}/{item}"

        try:
            stat_info = sftp.stat(remote_item_path)
            if stat_info.st_mode & 0o40000:  # S_ISDIR
                _get_all_files_recursive(sftp, remote_item_path, dest_item_path, file_list)
            else:
                file_list.append((remote_item_path, dest_item_path))
        except FileNotFoundError:
            logging.warning(f"File or directory vanished during scan: {remote_item_path}")
            continue

@retry(tries=2, delay=5)
def _sftp_upload_file(source_sftp_config, dest_sftp_config, source_file_path, dest_file_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_id, dry_run=False):
    """
    Streams a single file from a source SFTP server to a destination SFTP server with a progress bar.
    Establishes its own SFTP sessions for thread safety. Supports resuming.
    """
    file_name = os.path.basename(source_file_path)
    source_sftp, source_ssh = None, None
    dest_sftp, dest_ssh = None, None
    upload_successful = False

    try:
        # Connect to both servers
        source_sftp, source_ssh = connect_sftp(source_sftp_config)
        dest_sftp, dest_ssh = connect_sftp(dest_sftp_config)

        # Get source file size
        try:
            source_stat = source_sftp.stat(source_file_path)
            total_size = source_stat.st_size
        except FileNotFoundError:
            logging.warning(f"Source file not found, skipping: {source_file_path}")
            with count_lock:
                file_counts[parent_task_id][0] += 1
            return

        if total_size == 0:
            logging.warning(f"Skipping zero-byte source file: {file_name}")
            with count_lock:
                file_counts[parent_task_id][0] += 1
            return

        # Check destination file size for resuming
        dest_size = 0
        try:
            dest_stat = dest_sftp.stat(dest_file_path)
            dest_size = dest_stat.st_size
            if dest_size == total_size:
                logging.info(f"Skipping (exists and size matches): {file_name}")
                job_progress.update(parent_task_id, advance=total_size)
                overall_progress.update(overall_task_id, advance=total_size)
                if file_task_id is not None:
                    job_progress.update(file_task_id, completed=total_size)
                with count_lock:
                    file_counts[parent_task_id][0] += 1
                return
            elif dest_size > total_size:
                logging.warning(f"Destination file '{file_name}' is larger than source ({dest_size} > {total_size}). Re-uploading.")
                dest_size = 0
            else:
                logging.info(f"Resuming upload for {file_name} from {dest_size / (1024*1024):.2f} MB.")
        except FileNotFoundError:
            # File doesn't exist on destination, start new upload
            pass

        effective_task_id = file_task_id if file_task_id is not None else parent_task_id
        job_progress.update(effective_task_id, completed=dest_size)
        if dest_size > 0:
            if effective_task_id != parent_task_id:
                job_progress.update(parent_task_id, advance=dest_size)
            overall_progress.update(overall_task_id, advance=dest_size)

        if dry_run:
            logging.info(f"[DRY RUN] Would upload: {source_file_path} -> {dest_file_path}")
            remaining_size = total_size - dest_size
            job_progress.update(parent_task_id, advance=remaining_size)
            overall_progress.update(overall_task_id, advance=remaining_size)
            with count_lock:
                file_counts[parent_task_id][0] += 1
            return

        # Ensure destination directory exists
        dest_dir = os.path.dirname(dest_file_path)
        try:
            dest_sftp.stat(dest_dir)
        except FileNotFoundError:
            # A simple recursive mkdir
            parts = dest_dir.replace('\\', '/').split('/')
            current_dir = ''
            for part in parts:
                if not part: continue # Handles leading slash
                current_dir = f"{current_dir}/{part}"
                try:
                    dest_sftp.stat(current_dir)
                except FileNotFoundError:
                    dest_sftp.mkdir(current_dir)

        if file_task_id is not None:
            job_progress.update(file_task_id, visible=True)
            job_progress.start_task(file_task_id)

        try:
            with source_sftp.open(source_file_path, 'rb') as source_f:
                source_f.seek(dest_size)
                source_f.prefetch()
                # Open destination file in append mode if resuming
                with dest_sftp.open(dest_file_path, 'ab' if dest_size > 0 else 'wb') as dest_f:
                    while True:
                        chunk = source_f.read(32768)
                        if not chunk:
                            break
                        dest_f.write(chunk)
                        increment = len(chunk)
                        if effective_task_id != parent_task_id:
                            job_progress.update(parent_task_id, advance=increment)
                        job_progress.update(effective_task_id, advance=increment)
                        overall_progress.update(overall_task_id, advance=increment)

            final_dest_size = dest_sftp.stat(dest_file_path).st_size
            if final_dest_size == total_size:
                logging.info(f"Upload of '{file_name}' completed.")
                upload_successful = True
            else:
                raise Exception(f"Final size mismatch for {file_name}. Expected {total_size}, got {final_dest_size}")

        except PermissionError as e:
            logging.error(f"Permission denied on destination server. Check user permissions for path: {dest_file_path}")
            logging.error(f"The user '{dest_sftp_config['username']}' may not have write access to that directory on the destination server.")
            raise e
        except Exception as e:
            logging.error(f"Upload failed for {file_name}: {e}")
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
                if upload_successful:
                    job_progress.update(file_task_id, description=f"└─ [green]✓[/green] [cyan]{file_name}[/]")

    finally:
        if source_sftp: source_sftp.close()
        if source_ssh: source_ssh.close()
        if dest_sftp: dest_sftp.close()
        if dest_ssh: dest_ssh.close()

@retry(tries=2, delay=5)
def _sftp_download_file(sftp_config, remote_file, local_file, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_id, dry_run=False):
    """
    Downloads a single file with a progress bar, with retries. Establishes its own SFTP session
    to ensure thread safety when called from a ThreadPoolExecutor.
    Uses a pre-existing task_id for the file progress bar.
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
                logging.debug(f"SFTP SKIP: Local: {local_size}, Remote: {total_size}. Skipping file '{file_name}'.")
                job_progress.update(parent_task_id, advance=total_size)
                overall_progress.update(overall_task_id, advance=total_size)
                if file_task_id is not None:
                    job_progress.update(file_task_id, completed=total_size)
                with count_lock:
                    file_counts[parent_task_id][0] += 1
                return
            elif local_size > total_size:
                logging.warning(f"Local file '{file_name}' is larger than remote ({local_size} > {total_size}), re-downloading from scratch.")
                logging.debug(f"SFTP OVERWRITE: Local: {local_size}, Remote: {total_size}. Overwriting file '{file_name}'.")
                local_size = 0
            else:  # local_size < total_size
                logging.info(f"Resuming download for {file_name} from {local_size / (1024*1024):.2f} MB.")
                logging.debug(f"SFTP RESUME: Local: {local_size}, Remote: {total_size}. Resuming file '{file_name}'.")
        else:
            logging.debug(f"SFTP NEW: Local file '{local_file}' does not exist. Starting new download.")

        effective_task_id = file_task_id if file_task_id is not None else parent_task_id

        # If resuming, set the starting point for the file's progress bar.
        job_progress.update(effective_task_id, completed=local_size)

        if local_size > 0:
            # For multi-file torrents, we also need to advance the parent task bar
            # to reflect the resumed portion of this specific file. For single-file
            # torrents, the parent task is the effective task, which is already set.
            if effective_task_id != parent_task_id:
                job_progress.update(parent_task_id, advance=local_size)
            # Always advance the overall progress bar by the size of the resumed portion.
            overall_progress.update(overall_task_id, advance=local_size)

        if dry_run:
            logging.info(f"[DRY RUN] Would download: {remote_file} -> {local_path}")
            # In dry run, we simulate a full download by advancing the remaining amount
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
            # Manual download loop to support resuming
            mode = 'r+b' if local_size > 0 else 'wb'
            with sftp.open(remote_file, 'rb') as remote_f:
                remote_f.seek(local_size)
                remote_f.prefetch()  # Helps with performance
                with open(local_path, mode) as local_f:
                    if local_size > 0:
                        local_f.seek(local_size)
                    while True:
                        chunk = remote_f.read(32768)
                        if not chunk:
                            break
                        local_f.write(chunk)

                        increment = len(chunk)
                        # Advance the file-specific progress bar (or parent if single-file)
                        job_progress.update(effective_task_id, advance=increment)

                        # For multi-file torrents, we also advance the parent task.
                        # For single-file torrents, effective_task_id IS parent_task_id, so we avoid double-counting.
                        if effective_task_id != parent_task_id:
                            job_progress.update(parent_task_id, advance=increment)

                        # Always advance the overall progress
                        overall_progress.update(overall_task_id, advance=increment)

            # Final check
            final_local_size = local_path.stat().st_size
            if final_local_size == total_size:
                logging.info(f"Download of '{file_name}' completed.")
                download_successful = True
            else:
                # This could happen if the remote file changed size during transfer
                raise Exception(f"Final size mismatch for {file_name}. Expected {total_size}, got {final_local_size}")

        except PermissionError as e:
            logging.error(f"Permission denied while trying to write to local path: {local_path.parent}")
            logging.error("Please check that the user running the script has write permissions for this directory.")
            logging.error("If you intended to transfer to another remote server, use 'transfer_mode = sftp_upload' in your config.")
            raise e
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
        if ssh_client:
            ssh_client.close()

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
        "-a", "--partial", "--inplace",
        "--info=progress2",
        "--timeout=60",  # Exit if no data transferred for 60 seconds
        # Use the centralized SSH command function for multiplexing
        "-e", _get_ssh_command(port),
        remote_spec,
        local_parent_dir
    ]

    # Create a safe version of the command for logging, with the password obfuscated
    safe_rsync_cmd = list(rsync_cmd)
    safe_rsync_cmd[2] = "'********'"

    if dry_run:
        logging.info(f"[DRY RUN] Would execute rsync for: {os.path.basename(remote_path)}")
        logging.debug(f"[DRY RUN] Command: {' '.join(safe_rsync_cmd)}")
        task = job_progress.tasks[parent_task_id]
        job_progress.update(parent_task_id, advance=task.total)
        overall_progress.update(overall_task_id, advance=task.total)
        return

    # Check if the destination path exists to provide a clearer log message for resumes.
    if Path(local_path).exists():
        logging.info(f"Partial file/directory found for '{os.path.basename(remote_path)}'. Resuming with rsync.")
    else:
        logging.info(f"Starting rsync transfer for '{os.path.basename(remote_path)}'")
    logging.debug(f"Executing rsync command: {' '.join(safe_rsync_cmd)}")

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
                if "permission denied" in stderr_output.lower():
                    logging.error(f"Rsync failed due to a permission error on the local machine.")
                    logging.error(f"Please check that the user running the script has write permissions for the destination path: {local_parent_dir}")
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


def transfer_content(sftp_config, sftp, remote_path, local_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_map, max_concurrent_transfers, dry_run=False):
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

        with ThreadPoolExecutor(max_workers=max_concurrent_transfers) as file_executor:
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

def transfer_content_sftp_upload(source_sftp_config, dest_sftp_config, source_sftp, source_path, dest_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_map, max_concurrent_transfers, dry_run=False):
    """
    Transfers a remote file or directory from a source SFTP to a destination SFTP server.
    """
    source_stat = source_sftp.stat(source_path)
    if source_stat.st_mode & 0o40000:  # S_ISDIR
        all_files = []
        _get_all_files_recursive(source_sftp, source_path, dest_path, all_files)

        with count_lock:
            file_counts[parent_task_id] = [0, len(all_files)]

        with ThreadPoolExecutor(max_workers=max_concurrent_transfers) as file_executor:
            futures = [
                file_executor.submit(_sftp_upload_file, source_sftp_config, dest_sftp_config, source_f, dest_f, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_map.get(source_f), dry_run)
                for source_f, dest_f in all_files
            ]
            for future in as_completed(futures):
                future.result()  # Raise exceptions if any occurred
    else:  # It's a single file
        with count_lock:
            file_counts[parent_task_id] = [0, 1]
        _sftp_upload_file(source_sftp_config, dest_sftp_config, source_path, dest_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, None, dry_run)

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


def analyze_torrent(torrent, sftp_config, transfer_mode, live_console, sftp_client=None, ssh_client=None):
    """
    Analyzes a single torrent to determine its size.
    Can reuse existing SFTP/SSH client connections.
    Returns a tuple of (torrent, size). Size is None if calculation fails.
    """
    name = torrent.name
    remote_content_path = torrent.content_path
    logging.info(f"Analyzing torrent: {name}")
    total_size = None

    try:
        # Prioritize the fastest method if an SSH client is available
        if ssh_client:
            total_size = get_remote_size_du(ssh_client, remote_content_path)
        elif transfer_mode == 'rsync':
            total_size = get_remote_size_rsync(sftp_config, remote_content_path)
        else:  # sftp or sftp_upload mode, but no pre-existing ssh_client
            # If no sftp client is provided, create a temporary one.
            if sftp_client is None:
                temp_sftp, temp_ssh = None, None
                try:
                    # We can use the ssh client from this new connection for 'du'
                    temp_sftp, temp_ssh = connect_sftp(sftp_config)
                    total_size = get_remote_size_du(temp_ssh, remote_content_path)
                finally:
                    if temp_sftp: temp_sftp.close()
                    if temp_ssh: temp_ssh.close()
            # Otherwise, use the provided sftp client (slower method)
            else:
                total_size = get_remote_size(sftp_client, remote_content_path)

    except Exception as e:
        live_console.log(f"[bold red]Error calculating size for '{name}': {e}[/]")
        live_console.log(f"[yellow]Warning: Skipping torrent due to size calculation failure.[/]")
        return torrent, None

    if total_size == 0:
        live_console.log(f"[yellow]Warning: Skipping zero-size torrent: {name}[/]")

    return torrent, total_size


def transfer_torrent(torrent, total_size, source_qbit, destination_qbit, config, tracker_rules, job_progress, overall_progress, overall_task_id, file_counts, count_lock, task_add_lock, dry_run=False, test_run=False):
    """
    Executes the transfer and management process for a single, pre-analyzed torrent.
    """
    name, hash = torrent.name, torrent.hash
    source_sftp, source_ssh = None, None
    source_paused = False
    parent_task_id = None
    success = False
    try:
        dest_base_path = config['DESTINATION_PATHS']['destination_path']
        remote_dest_base_path = config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path

        source_content_path = torrent.content_path
        content_name = os.path.basename(source_content_path)
        dest_content_path = os.path.join(dest_base_path, content_name)

        transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
        source_sftp_config = config['SOURCE_SERVER']

        with task_add_lock:
            parent_task_id = job_progress.add_task(name, total=total_size, start=True)

        # --- Execute Transfer ---
        if transfer_mode == 'rsync':
            transfer_content_rsync(source_sftp_config, source_content_path, dest_content_path, job_progress, parent_task_id, overall_progress, overall_task_id, dry_run)
            logging.info(f"Rsync transfer completed successfully for '{name}'.")
        else:  # sftp or sftp_upload
            source_sftp, source_ssh = connect_sftp(source_sftp_config)
            all_files = []
            source_stat = source_sftp.stat(source_content_path)
            if source_stat.st_mode & 0o40000:  # S_ISDIR
                _get_all_files_recursive(source_sftp, source_content_path, dest_content_path, all_files)
            else:  # It's a single file
                all_files.append((source_content_path, dest_content_path))

            file_task_map = {}
            if len(all_files) > 1:
                with task_add_lock:
                    for remote_f, _ in all_files:
                        file_name = os.path.basename(remote_f)
                        try:
                            file_size = source_sftp.stat(remote_f).st_size
                        except FileNotFoundError:
                            file_size = 0
                        task_id = job_progress.add_task(f"└─ [cyan]{file_name}[/]", total=file_size, start=False, visible=False)
                        file_task_map[remote_f] = task_id

            max_concurrent_transfers = config['SETTINGS'].getint('max_concurrent_file_transfers', 5)

            if transfer_mode == 'sftp_upload':
                logging.info(f"Starting SFTP-to-SFTP upload for '{name}'")
                dest_sftp_config = config['DESTINATION_SERVER']
                transfer_content_sftp_upload(source_sftp_config, dest_sftp_config, source_sftp, source_content_path, dest_content_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_map, max_concurrent_transfers, dry_run)
                logging.info(f"SFTP-to-SFTP upload completed for '{name}'.")
            else: # Default 'sftp' download
                logging.info(f"Starting SFTP download for '{name}'")
                transfer_content(source_sftp_config, source_sftp, source_content_path, dest_content_path, job_progress, parent_task_id, overall_progress, overall_task_id, file_counts, count_lock, file_task_map, max_concurrent_transfers, dry_run)
                logging.info(f"SFTP download completed successfully for '{name}'.")

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
        if sftp:
            sftp.close()
        if ssh_client:
            ssh_client.close()
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
                # If rule applied or torrent already in correct category, skip manual interaction
                continue

            # --- Start of interactive part for this torrent ---
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

    # Get the root logger
    logger = logging.getLogger()
    log_level = logging.DEBUG if debug else logging.INFO
    logger.setLevel(log_level)

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

def test_path_permissions(path_to_test, remote_config=None):
    """
    Tests write permissions for a given path by creating and deleting a temporary file.
    If 'remote_config' is provided, it performs the test on the remote SFTP server.
    Otherwise, it tests the local path.
    """
    if remote_config:
        logging.info(f"--- Running REMOTE Permission Test on: {path_to_test} ---")
        sftp, ssh = None, None
        try:
            sftp, ssh = connect_sftp(remote_config)
            # Normalize to use forward slashes for remote paths
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
            logging.error(f"[bold red]FAILURE:[/] Permission denied on remote server when trying to write to '{path_to_test}'.")
            logging.error(f"Please ensure the user '{remote_config['username']}' has write access to this path on the remote server.")
            return False
        except Exception as e:
            logging.error(f"[bold red]FAILURE:[/] An unexpected error occurred during remote permission test: {e}", exc_info=True)
            return False
        finally:
            if sftp: sftp.close()
            if ssh: ssh.close()
    else:
        p = Path(path_to_test)
        logging.info(f"--- Running LOCAL Permission Test on: {p} ---")

        if not p.exists():
            logging.error(f"Error: The local path '{p}' does not exist.")
            logging.error("Please ensure the base directory is created and accessible.")
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
            logging.error(f"[bold red]FAILURE:[/] Permission denied when trying to write to local path '{p}'.")
            logging.error(f"Please ensure the user '{getpass.getuser()}' running the script has write access to this path.")
            return False
        except Exception as e:
            logging.error(f"[bold red]FAILURE:[/] An unexpected error occurred during local permission test: {e}", exc_info=True)
            return False

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
    parser.add_argument('-l', '--list-rules', action='store_true', help='List all tracker-to-category rules and exit.')
    parser.add_argument('-a', '--add-rule', nargs=2, metavar=('TRACKER_DOMAIN', 'CATEGORY'), help='Add or update a rule and exit.')
    parser.add_argument('-d', '--delete-rule', metavar='TRACKER_DOMAIN', help='Delete a rule and exit.')
    parser.add_argument('-c', '--categorize', dest='interactive_categorize', action='store_true', help='Interactively categorize torrents on destination.')
    parser.add_argument('--category', help='(For -c mode) Specify a category to scan, overriding the config.')
    parser.add_argument('-nr', '--no-rules', action='store_true', help='(For -c mode) Ignore existing rules and show all torrents in the category.')
    parser.add_argument('--test-permissions', action='store_true', help='Test write permissions for the configured destination_path and exit.')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    setup_logging(script_dir, args.dry_run, args.test_run, args.debug)

    # Check for config updates before loading it
    config_template_path = script_dir / 'config.ini.template'
    update_config(args.config, str(config_template_path))

    # --- Early exit argument handling ---
    if args.list_rules or args.add_rule or args.delete_rule or args.interactive_categorize or args.test_permissions:
        config = load_config(args.config)
        tracker_rules = load_tracker_rules(script_dir)
        logging.info("Executing utility command...")

        if args.test_permissions:
            transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
            dest_path = config['DESTINATION_PATHS'].get('destination_path')
            if not dest_path:
                logging.error("FATAL: 'destination_path' is not defined in your config file.")
                return 1

            remote_config = None
            if transfer_mode == 'sftp_upload':
                if 'DESTINATION_SERVER' not in config:
                    logging.error("FATAL: 'transfer_mode' is 'sftp_upload' but [DESTINATION_SERVER] is not defined in config.")
                    return 1
                remote_config = config['DESTINATION_SERVER']

            test_path_permissions(dest_path, remote_config=remote_config)
            return 0

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
                dest_client_section = config['SETTINGS'].get('destination_client_section', 'DESTINATION_QBITTORRENT')
                destination_qbit = connect_qbit(config[dest_client_section], "Destination")
                # Determine the category to scan
                if args.category:
                    cat_to_scan = args.category
                    logging.info(f"Using category from command line: '{cat_to_scan}'")
                else:
                    cat_to_scan = config['SETTINGS'].get('category_to_move')
                    if cat_to_scan:
                        logging.info(f"Using 'category_to_move' from config: '{cat_to_scan}'")
                    else:
                        logging.warning("No category specified via --category or in config. Defaulting to 'uncategorized'.")
                        cat_to_scan = '' # Empty string scans for uncategorized

                run_interactive_categorization(destination_qbit, tracker_rules, script_dir, cat_to_scan, args.no_rules)
            except Exception as e:
                logging.error(f"Failed to run interactive categorization: {e}", exc_info=True)
            return 0
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
            source_section_name = config['SETTINGS']['source_client_section']
            dest_section_name = config['SETTINGS']['destination_client_section']

            source_qbit = connect_qbit(config[source_section_name], "Source")
            destination_qbit = connect_qbit(config[dest_section_name], "Destination")
        except KeyError as e:
            logging.error(f"Configuration Error: The client section '{e}' is defined in your [SETTINGS] but not found in the config file.")
            logging.error("Please ensure 'source_client_section' and 'destination_client_section' in [SETTINGS] match the actual section names (e.g., [SOURCE_CLIENT]).")
            return 1
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

        eligible_torrents = get_eligible_torrents(source_qbit, category_to_move, size_threshold_gb)
        if not eligible_torrents:
            logging.info("No torrents to move at this time.")
            return 0
        total_count = len(eligible_torrents)
        processed_count = 0

        # --- UI Setup ---
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

        layout = Group(
            plan_panel,
            Panel(Group(torrent_progress, analysis_progress), title="Overall Queue", border_style="blue"),
            Panel(overall_progress, title="Total Progress", border_style="green"),
            Panel(job_progress, title="Active Transfers", border_style="yellow", padding=(1, 2))
        )

        with Live(layout, refresh_per_second=4, transient=True) as live:
            sftp_config = config['SOURCE_SERVER']
            transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

            analysis_workers = max(10, args.parallel_jobs * 2)

            sftp_analysis_client, ssh_analysis_client = None, None
            try:
                # If using an SFTP-based transfer mode, create one client for all analysis jobs
                if transfer_mode in ['sftp', 'sftp_upload']:
                    logging.info("Pre-connecting to source SFTP server for analysis...")
                    sftp_analysis_client, ssh_analysis_client = connect_sftp(sftp_config)

                with ThreadPoolExecutor(max_workers=analysis_workers, thread_name_prefix='Analyzer') as analysis_executor, \
                     ThreadPoolExecutor(max_workers=args.parallel_jobs, thread_name_prefix='Transfer') as transfer_executor:

                    analysis_future_to_torrent = {
                        analysis_executor.submit(analyze_torrent, torrent, sftp_config, transfer_mode, live.console, sftp_client=sftp_analysis_client, ssh_client=ssh_analysis_client): torrent
                        for torrent in eligible_torrents
                    }
                    transfer_future_to_torrent = {}

                    # Pipeline: As analysis completes, feed into the transfer pool
                    for future in as_completed(analysis_future_to_torrent):
                        original_torrent = analysis_future_to_torrent[future]
                        try:
                            analyzed_torrent, total_size = future.result()
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
                                    transfer_torrent, analyzed_torrent, total_size,
                                    source_qbit, destination_qbit, config, tracker_rules,
                                    job_progress, overall_progress, overall_task,
                                    file_counts, count_lock, task_add_lock,
                                    args.dry_run, args.test_run
                                )
                                transfer_future_to_torrent[transfer_future] = analyzed_torrent
                            else:
                                torrent_progress.update(torrent_task, advance=1)

                        except Exception as e:
                            live.console.log(f"[bold red]Error processing analysis for '{original_torrent.name}': {e}[/]")
                            analysis_progress.update(analysis_task, advance=1)
                            torrent_progress.update(torrent_task, advance=1)

                    live.console.log("[green]All torrents analyzed. Waiting for transfers to complete...[/]")

                    # Wait for all transfers to complete
                    for future in as_completed(transfer_future_to_torrent):
                        torrent = transfer_future_to_torrent[future]
                        try:
                            if future.result():
                                processed_count += 1
                        except Exception as e:
                            live.console.log(f"[bold red]An exception was thrown for torrent '{torrent.name}': {e}[/]", exc_info=True)
                        finally:
                            torrent_progress.update(torrent_task, advance=1)
                            if len(job_progress.tasks) > 0 and job_progress.tasks[-1].description != " ":
                                job_progress.add_task(" ", total=1, completed=1)
            except KeyboardInterrupt:
                # The executors will be shut down by the 'with' statement context exit
                live.stop()
                live.console.print("\n[bold yellow]Process interrupted by user. Transfers will be cancelled.[/bold yellow]")
                # No need to manually shutdown, the `with` block handles it.
                raise # Re-raise to be caught by the outer try/except
            finally:
                # Clean up the dedicated analysis SFTP client if it was created
                if sftp_analysis_client:
                    logging.info("Closing analysis SFTP connection.")
                    sftp_analysis_client.close()
                if ssh_analysis_client:
                    ssh_analysis_client.close()
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