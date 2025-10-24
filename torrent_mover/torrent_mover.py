#!/usr/bin/env python3
# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

__version__ = "1.6.0"

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
from .utils import retry, SSHConnectionPool, LockFile
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
from .ui import UIManager


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
        logging.debug(f"Using SSH control path: {SSH_CONTROL_PATH}")
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
    logging.debug("'sshpass' dependency check passed.")
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
    logging.info("STATE: Checking for configuration updates...")

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
                logging.info(f"CONFIG: Added new section to config: [{section_name}]")
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
                        logging.info(f"CONFIG: Added new option in [{section_name}]: {key}")

        if changes_made:
            # Create a backup in a 'backup' sub-folder
            backup_dir = config_file.parent / 'backup'
            backup_dir.mkdir(exist_ok=True)
            backup_filename = f"{config_file.stem}.bak_{time.strftime('%Y%m%d-%H%M%S')}"
            backup_path = backup_dir / backup_filename
            shutil.copy2(config_file, backup_path)
            logging.info(f"CONFIG: Backed up existing configuration to '{backup_path}'")
            with config_file.open('w', encoding='utf-8') as f:
                updater.write(f)
            logging.info("CONFIG: Configuration file has been updated with new options.")
        else:
            logging.info("CONFIG: Configuration file is already up-to-date.")

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

    logging.info(f"STATE: Connecting to {client_name} qBittorrent at {host}...")
    client = qbittorrentapi.Client(
        host=host,
        port=port,
        username=username,
        password=password,
        VERIFY_WEBUI_CERTIFICATE=verify_cert,
        REQUESTS_ARGS={'timeout': 10} # Add a timeout to the underlying requests
    )
    client.auth_log_in()
    logging.info(f"CLIENT: Successfully connected to {client_name}. Version: {client.app.version}")
    return client

# --- SFTP Transfer Logic with Progress Bar ---

def sftp_mkdir_p(sftp, remote_path):
    """
    Ensures a directory exists on the remote SFTP server, creating it recursively if necessary.
    This is similar to `mkdir -p`.
    """
    if not remote_path:
        return
    # Normalize to forward slashes and remove any trailing slash
    remote_path = remote_path.replace('\\', '/').rstrip('/')

    try:
        # Check if the path already exists and is a directory
        sftp.stat(remote_path)
    except FileNotFoundError:
        # Path does not exist, so create it
        parent_dir = os.path.dirname(remote_path)
        sftp_mkdir_p(sftp, parent_dir) # Recurse
        try:
            sftp.mkdir(remote_path)
        except IOError as e:
            # This can happen due to race conditions or permission issues
            logging.error(f"Failed to create remote directory '{remote_path}': {e}")
            # Re-check if it was created by another thread just in case
            try:
                sftp.stat(remote_path)
            except FileNotFoundError:
                # If it still doesn't exist, the error is genuine
                raise e

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
def _get_remote_size_du_core(ssh_client, remote_path):
    """
    Core logic for getting remote size using 'du -sb'. Does not handle concurrency.
    This function is retried on failure.
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


def is_remote_dir(ssh_client, path):
    """Checks if a remote path is a directory using 'test -d'."""
    try:
        # Escape single quotes to prevent command injection
        escaped_path = path.replace("'", "'\\''")
        command = f"test -d '{escaped_path}'"
        stdin, stdout, stderr = ssh_client.exec_command(command, timeout=30)
        exit_status = stdout.channel.recv_exit_status()
        return exit_status == 0
    except Exception as e:
        logging.error(f"Error checking if remote path '{path}' is a directory: {e}")
        # Default to assuming it's not a directory on error for safety
        return False


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
def _sftp_download_to_cache(source_pool, source_file_path, local_cache_path, torrent_hash, ui):
    """
    Downloads a file from SFTP to a local cache path, with resume support and progress reporting.
    """
    try:
        with source_pool.get_connection() as (sftp, ssh):
            remote_stat = sftp.stat(source_file_path)
            total_size = remote_stat.st_size

            ui.start_file_transfer(torrent_hash, source_file_path, total_size)
            ui.update_file_status(torrent_hash, source_file_path, "Downloading")

            local_size = 0
            if local_cache_path.exists():
                local_size = local_cache_path.stat().st_size

            if local_size >= total_size:
                logging.info(f"Cache hit, skipping download: {os.path.basename(source_file_path)}")
                ui.update_torrent_byte_progress(torrent_hash, total_size)
                ui.advance_overall_progress(total_size)
                ui.update_file_progress(torrent_hash, source_file_path, total_size)
                return

            if local_size > 0:
                logging.info(f"Resuming download to cache: {os.path.basename(source_file_path)}")
                mode = 'ab'
                ui.update_torrent_byte_progress(torrent_hash, local_size)
                ui.advance_overall_progress(local_size)
                ui.update_file_progress(torrent_hash, source_file_path, local_size)
            else:
                logging.info(f"Downloading to cache: {os.path.basename(source_file_path)}")
                mode = 'wb'

            with sftp.open(source_file_path, 'rb') as remote_f:
                remote_f.seek(local_size)
                remote_f.prefetch()
                with open(local_cache_path, mode) as local_f:
                    while True:
                        chunk = remote_f.read(65536)
                        if not chunk: break
                        local_f.write(chunk)
                        increment = len(chunk)
                        ui.update_torrent_byte_progress(torrent_hash, increment)
                        ui.advance_overall_progress(increment)
                        ui.update_file_progress(torrent_hash, source_file_path, increment)

    except Exception as e:
        logging.error(f"Failed to download to cache for {source_file_path}: {e}")
        raise

@retry(tries=2, delay=5)
def _sftp_upload_from_cache(dest_pool, local_cache_path, source_file_path, dest_file_path, torrent_hash, ui):
    """
    Uploads a file from a local cache path to the destination SFTP server.
    """
    file_name = local_cache_path.name

    if not local_cache_path.is_file():
        logging.error(f"Cannot upload '{file_name}' from cache: File does not exist.")
        ui.update_file_status(torrent_hash, source_file_path, "[red]Cache file missing[/red]")
        ui.advance_torrent_file_progress(torrent_hash)
        raise FileNotFoundError(f"Local cache file not found: {local_cache_path}")

    try:
        total_size = local_cache_path.stat().st_size
        ui.start_file_transfer(torrent_hash, source_file_path, total_size)
        ui.update_file_status(torrent_hash, source_file_path, "Uploading")

        with dest_pool.get_connection() as (sftp, ssh):
            dest_size = 0
            try:
                dest_size = sftp.stat(dest_file_path).st_size
            except FileNotFoundError:
                pass

            if dest_size >= total_size:
                logging.info(f"Skipping upload (exists and size matches): {file_name}")
                ui.update_torrent_byte_progress(torrent_hash, total_size)
                ui.advance_overall_progress(total_size)
                ui.update_file_progress(torrent_hash, source_file_path, total_size)
                return

            if dest_size > 0:
                ui.update_torrent_byte_progress(torrent_hash, dest_size)
                ui.advance_overall_progress(dest_size)
                ui.update_file_progress(torrent_hash, source_file_path, dest_size)


            dest_dir = os.path.dirname(dest_file_path)
            sftp_mkdir_p(sftp, dest_dir)

            with open(local_cache_path, 'rb') as source_f:
                source_f.seek(dest_size)
                with sftp.open(dest_file_path, 'ab' if dest_size > 0 else 'wb') as dest_f:
                    while True:
                        chunk = source_f.read(32768)
                        if not chunk: break
                        dest_f.write(chunk)
                        increment = len(chunk)
                        ui.update_torrent_byte_progress(torrent_hash, increment)
                        ui.advance_overall_progress(increment)
                        ui.update_file_progress(torrent_hash, source_file_path, increment)

            if sftp.stat(dest_file_path).st_size != total_size:
                raise Exception("Final size mismatch during cached upload.")

            ui.update_file_status(torrent_hash, source_file_path, "[green]Completed[/green]")

    except Exception as e:
        ui.update_file_status(torrent_hash, source_file_path, "[red]Upload Failed[/red]")
        logging.error(f"Upload from cache failed for {file_name}: {e}")
        raise
    finally:
        ui.advance_torrent_file_progress(torrent_hash)

def _sftp_upload_file(source_pool, dest_pool, source_file_path, dest_file_path, torrent_hash, ui, dry_run=False):
    """
    Streams a single file from a source SFTP server to a destination SFTP server with a progress bar.
    Establishes its own SFTP sessions for thread safety. Supports resuming.
    """
    file_name = os.path.basename(source_file_path)
    try:
        with source_pool.get_connection() as (source_sftp, source_ssh), dest_pool.get_connection() as (dest_sftp, dest_ssh):
            start_time = time.time()
            # Get source file size
            try:
                source_stat = source_sftp.stat(source_file_path)
                total_size = source_stat.st_size
            except FileNotFoundError:
                logging.warning(f"Source file not found, skipping: {source_file_path}")
                return

            if total_size == 0:
                logging.warning(f"Skipping zero-byte source file: {file_name}")
                return

            # Check destination file size for resuming
            dest_size = 0
            try:
                dest_stat = dest_sftp.stat(dest_file_path)
                dest_size = dest_stat.st_size
                if dest_size == total_size:
                    logging.info(f"Skipping (exists and size matches): {file_name}")
                    ui.update_torrent_byte_progress(torrent_hash, total_size - dest_size)
                    ui.advance_overall_progress(total_size - dest_size)
                    return
                elif dest_size > total_size:
                    logging.warning(f"Destination file '{file_name}' is larger than source ({dest_size} > {total_size}). Re-uploading.")
                    dest_size = 0
                else:
                    logging.info(f"Resuming upload for {file_name} from {dest_size / (1024*1024):.2f} MB.")
            except FileNotFoundError:
                pass # File doesn't exist on destination, start new upload

            if dest_size > 0:
                ui.update_torrent_byte_progress(torrent_hash, dest_size)
                ui.advance_overall_progress(dest_size)

        if dry_run:
            logging.info(f"[DRY RUN] Would upload: {source_file_path} -> {dest_file_path}")
            remaining_size = total_size - dest_size
            ui.update_torrent_byte_progress(torrent_hash, remaining_size)
            ui.advance_overall_progress(remaining_size)
            return

        # Ensure destination directory exists
        dest_dir = os.path.dirname(dest_file_path)
        sftp_mkdir_p(dest_sftp, dest_dir)

        try:
            with source_sftp.open(source_file_path, 'rb') as source_f:
                source_f.seek(dest_size)
                source_f.prefetch()
                # Open destination file in append mode if resuming
                with dest_sftp.open(dest_file_path, 'ab' if dest_size > 0 else 'wb') as dest_f:
                    while True:
                        chunk = source_f.read(32768)
                        if not chunk: break
                        dest_f.write(chunk)
                        increment = len(chunk)
                        ui.update_torrent_byte_progress(torrent_hash, increment)
                        ui.advance_overall_progress(increment)

            final_dest_size = dest_sftp.stat(dest_file_path).st_size
            if final_dest_size == total_size:
                end_time = time.time()
                duration = end_time - start_time
                logging.debug(f"Upload of '{file_name}' completed.")
                if duration > 0:
                    speed_mbps = (total_size * 8) / (duration * 1024 * 1024)
                    logging.debug(f"PERF: '{file_name}' ({total_size / 1024**2:.2f} MiB) took {duration:.2f} seconds.")
                    logging.debug(f"PERF: Average speed: {speed_mbps:.2f} Mbps.")
                else:
                    logging.debug(f"PERF: '{file_name}' completed in < 1 second.")
            else:
                raise Exception(f"Final size mismatch for {file_name}. Expected {total_size}, got {final_dest_size}")

        except PermissionError as e:
            logging.error(f"Permission denied on destination server for path: {dest_file_path}")
            logging.error("Please check that the destination user has write access to that directory.")
            raise e
        except Exception as e:
            logging.error(f"Upload failed for {file_name}: {e}")
            raise
    finally:
        ui.advance_torrent_file_progress(torrent_hash)


@retry(tries=2, delay=5)
def _sftp_download_file(pool, remote_file, local_file, torrent_hash, ui, dry_run=False):
    """
    Downloads a single file with a progress bar, with retries. Establishes its own SFTP session
    to ensure thread safety when called from a ThreadPoolExecutor.
    """
    local_path = Path(local_file)
    file_name = os.path.basename(remote_file)

    try:
        with pool.get_connection() as (sftp, ssh_client):
            start_time = time.time()
            remote_stat = sftp.stat(remote_file)
            total_size = remote_stat.st_size
            logging.debug(f"SFTP Check: Remote file '{remote_file}' size: {total_size}")

            if total_size == 0:
                logging.warning(f"Skipping zero-byte file: {file_name}")
                return

            local_size = 0
            if local_path.exists():
                local_size = local_path.stat().st_size
                logging.debug(f"SFTP Check: Local file '{local_file}' exists with size: {local_size}")
                if local_size == total_size:
                    logging.info(f"Skipping (exists and size matches): {file_name}")
                    logging.debug(f"SFTP SKIP: Local: {local_size}, Remote: {total_size}. Skipping file '{file_name}'.")
                    ui.update_torrent_byte_progress(torrent_hash, total_size - local_size)
                    ui.advance_overall_progress(total_size - local_size)
                    return
                elif local_size > total_size:
                    logging.warning(f"Local file '{file_name}' is larger than remote ({local_size} > {total_size}), re-downloading from scratch.")
                    logging.debug(f"SFTP OVERWRITE: Local: {local_size}, Remote: {total_size}. Overwriting file '{file_name}'.")
                    local_size = 0
                else:  # local_size < total_size
                    logging.info(f"Resuming download for {file_name} from {local_size / (1024*1024):.2f} MB.")
                    logging.debug(f"SFTP RESUME: Local: {local_size}, Remote: {total_size}. Resuming file '{file_name}'.")

        if local_size > 0:
            ui.update_torrent_byte_progress(torrent_hash, local_size)
            ui.advance_overall_progress(local_size)

        if dry_run:
            logging.info(f"[DRY RUN] Would download: {remote_file} -> {local_path}")
            remaining_size = total_size - local_size
            ui.update_torrent_byte_progress(torrent_hash, remaining_size)
            ui.advance_overall_progress(remaining_size)
            return

        local_path.parent.mkdir(parents=True, exist_ok=True)

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
                        if not chunk: break
                        local_f.write(chunk)
                        increment = len(chunk)
                        ui.update_torrent_byte_progress(torrent_hash, increment)
                        ui.advance_overall_progress(increment)

            # Final check
            final_local_size = local_path.stat().st_size
            if final_local_size == total_size:
                end_time = time.time()
                duration = end_time - start_time
                logging.debug(f"Download of '{file_name}' completed.")
                if duration > 0:
                    speed_mbps = (total_size * 8) / (duration * 1024 * 1024)
                    logging.debug(f"PERF: '{file_name}' ({total_size / 1024**2:.2f} MiB) took {duration:.2f} seconds.")
                    logging.debug(f"PERF: Average speed: {speed_mbps:.2f} Mbps.")
                else:
                    logging.debug(f"PERF: '{file_name}' completed in < 1 second.")
            else:
                raise Exception(f"Final size mismatch for {file_name}. Expected {total_size}, got {final_local_size}")

        except PermissionError as e:
            logging.error(f"Permission denied while trying to write to local path: {local_path.parent}")
            logging.error("Please check that the user running the script has write permissions for this directory.")
            logging.error("If you intended to transfer to another remote server, use 'transfer_mode = sftp_upload' in your config.")
            raise e
        except Exception as e:
            logging.error(f"Download failed for {file_name}: {e}")
            raise
    finally:
        ui.advance_torrent_file_progress(torrent_hash)


def transfer_content_rsync(sftp_config, remote_path, local_path, torrent_hash, ui, dry_run=False):
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

    rsync_cmd = [
        "sshpass", "-p", password,
        "rsync",
        "-a", "--partial", "--inplace",
        "--info=progress2",
        "--timeout=60",
        "-e", _get_ssh_command(port),
        remote_spec,
        local_parent_dir
    ]

    safe_rsync_cmd = list(rsync_cmd)
    safe_rsync_cmd[2] = "'********'"

    if dry_run:
        logging.info(f"[DRY RUN] Would execute rsync for: {os.path.basename(remote_path)}")
        logging.debug(f"[DRY RUN] Command: {' '.join(safe_rsync_cmd)}")
        ui.update_torrent_byte_progress(torrent_hash, ui._torrents_data[torrent_hash]["progress_obj"].tasks[0].total)
        ui.advance_overall_progress(ui._torrents_data[torrent_hash]["progress_obj"].tasks[0].total)
        return

    if Path(local_path).exists():
        logging.info(f"Partial file/directory found for '{os.path.basename(remote_path)}'. Resuming with rsync.")
    else:
        logging.info(f"Starting rsync transfer for '{os.path.basename(remote_path)}'")
    logging.debug(f"Executing rsync command: {' '.join(safe_rsync_cmd)}")

    max_retries = 2
    retry_delay = 5

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
                                ui.update_torrent_byte_progress(torrent_hash, advance)
                                ui.advance_overall_progress(advance)
                                last_total_transferred = total_transferred
                        except (ValueError, IndexError):
                            logging.warning(f"Could not parse rsync progress line: {line}")
                    else:
                        logging.debug(f"rsync stdout: {line}")

            process.wait()
            end_time = time.time()
            stderr_output = process.stderr.read() if process.stderr else ""

            if process.returncode == 0 or process.returncode == 24:
                if process.returncode == 24:
                    logging.warning(f"Rsync finished with code 24 (some source files vanished) for '{os.path.basename(remote_path)}'.")

                duration = end_time - start_time
                task = ui._torrents_data[torrent_hash]["progress_obj"].tasks[0]
                total_size_bytes = task.total
                if duration > 0:
                    speed_mbps = (total_size_bytes * 8) / (duration * 1024 * 1024)
                    logging.debug(f"PERF: '{os.path.basename(remote_path)}' ({total_size_bytes / 1024**2:.2f} MiB) took {duration:.2f} seconds.")
                    logging.debug(f"PERF: Average speed: {speed_mbps:.2f} Mbps.")
                else:
                    logging.debug(f"PERF: '{os.path.basename(remote_path)}' completed in < 1 second.")

                if task.completed < task.total:
                    remaining = task.total - task.completed
                    if remaining > 0:
                        ui.update_torrent_byte_progress(torrent_hash, remaining)
                        ui.advance_overall_progress(remaining)

                logging.info(f"Rsync transfer completed successfully for '{os.path.basename(remote_path)}'.")
                # For rsync, since it's a single operation, we mark all files as "transferred" at the end.
                ui.advance_torrent_file_progress(torrent_hash)
                return

            elif process.returncode == 30:
                logging.warning(f"Rsync timed out for '{os.path.basename(remote_path)}'. Retrying...")
                continue

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
            if attempt < max_retries:
                logging.warning("Retrying...")
                continue
            else:
                raise e

    raise Exception(f"Rsync transfer for '{os.path.basename(remote_path)}' failed after {max_retries} attempts.")


def transfer_content(pool, sftp, remote_path, local_path, torrent_hash, ui, max_concurrent_downloads, dry_run=False):
    """
    Transfers a remote file or directory to a local path, preserving structure.
    """
    remote_stat = sftp.stat(remote_path)
    if remote_stat.st_mode & 0o40000:  # S_ISDIR
        all_files = []
        _get_all_files_recursive(sftp, remote_path, local_path, all_files)

        with ThreadPoolExecutor(max_workers=max_concurrent_downloads) as file_executor:
            futures = [
                file_executor.submit(_sftp_download_file, pool, remote_f, local_f, torrent_hash, ui, dry_run)
                for remote_f, local_f in all_files
            ]
            for future in as_completed(futures):
                future.result()

    else: # It's a single file
        _sftp_download_file(pool, remote_path, local_path, torrent_hash, ui, dry_run)

def transfer_content_sftp_upload(
    source_pool, dest_pool, source_sftp, source_path, dest_path, torrent_hash, ui,
    max_concurrent_downloads, max_concurrent_uploads, dry_run=False, local_cache_sftp_upload=False
):
    """
    Transfers a remote file or directory from a source SFTP to a destination SFTP server.
    """
    source_stat = source_sftp.stat(source_path)
    all_files = []
    is_dir = source_stat.st_mode & 0o40000
    if is_dir:
        _get_all_files_recursive(source_sftp, source_path, dest_path, all_files)
    else: # It's a single file
        all_files.append((source_path, dest_path))

    if local_cache_sftp_upload:
        if dry_run:
            logging.info(f"[DRY RUN] Would download {len(all_files)} files to cache and then upload.")
            ui.update_torrent_byte_progress(torrent_hash, ui._torrents_data[torrent_hash]["progress_obj"].tasks[0].total)
            ui.advance_overall_progress(ui._torrents_data[torrent_hash]["progress_obj"].tasks[0].total)
            return

        temp_dir = Path(tempfile.gettempdir()) / f"torrent_mover_cache_{torrent_hash}"
        temp_dir.mkdir(exist_ok=True)
        transfer_successful = False
        try:
            with ThreadPoolExecutor(max_workers=max_concurrent_downloads, thread_name_prefix='CacheDownloader') as download_executor, \
                 ThreadPoolExecutor(max_workers=max_concurrent_uploads, thread_name_prefix='CacheUploader') as upload_executor:

                download_futures = {
                    download_executor.submit(
                        _sftp_download_to_cache, source_pool, source_f,
                        temp_dir / os.path.basename(source_f), torrent_hash, ui
                    ): (source_f, dest_f)
                    for source_f, dest_f in all_files
                }
                upload_futures = []

                for download_future in as_completed(download_futures):
                    source_f, dest_f = download_futures[download_future]
                    try:
                        download_future.result()
                        local_cache_path = temp_dir / os.path.basename(source_f)
                        upload_future = upload_executor.submit(
                            _sftp_upload_from_cache, dest_pool, local_cache_path,
                            source_f, dest_f, torrent_hash, ui
                        )
                        upload_futures.append(upload_future)
                    except Exception as e:
                        logging.error(f"Download of '{os.path.basename(source_f)}' failed, it will not be uploaded. Error: {e}")
                        # Re-raise the exception to be caught by the main transfer_torrent function
                        raise

                for upload_future in as_completed(upload_futures):
                    # This will re-raise any exception caught during the upload process
                    upload_future.result()

            transfer_successful = True
        finally:
            if transfer_successful:
                shutil.rmtree(temp_dir, ignore_errors=True)
                logging.debug(f"Successfully removed cache directory: {temp_dir}")

    else: # Streaming mode
        with ThreadPoolExecutor(max_workers=max_concurrent_uploads) as file_executor:
            futures = [
                file_executor.submit(
                    _sftp_upload_file, source_pool, dest_pool, source_f, dest_f,
                    torrent_hash, ui, dry_run
                )
                for source_f, dest_f in all_files
            ]
            for future in as_completed(futures):
                future.result()

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


def cleanup_orphaned_cache(destination_qbit):
    """
    Scans for leftover cache directories from previous runs and removes them
    if the corresponding torrent is already successfully on the destination client.
    """
    temp_dir = Path(tempfile.gettempdir())
    cache_prefix = "torrent_mover_cache_"
    logging.info("--- Running Orphaned Cache Cleanup ---")

    try:
        all_dest_hashes = {t.hash for t in destination_qbit.torrents_info() if t.progress == 1}
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


def recover_cached_torrents(source_qbit, destination_qbit):
    """
    Scans for leftover cache directories from previous runs and adds them back
    to the processing queue if the torrent is not on the destination.
    """
    temp_dir = Path(tempfile.gettempdir())
    cache_prefix = "torrent_mover_cache_"
    logging.info("--- Checking for incomplete cached transfers to recover ---")
    recovered_torrents = []

    try:
        all_dest_hashes = {t.hash for t in destination_qbit.torrents_info()}
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
    except Exception as e:
        logging.error(f"An error occurred while trying to get torrent info from the source for recovery: {e}")

    return recovered_torrents


def destination_health_check(config, total_transfer_size_bytes, ssh_connection_pools):
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
        if remote_config:
            with dest_pool.get_connection() as (sftp, ssh):
                escaped_path = dest_path.replace("'", "'\\''")
                command = f"df -kP '{escaped_path}'"
                stdin, stdout, stderr = ssh.exec_command(command, timeout=30)
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

        required_space_gb = total_transfer_size_bytes / (1024**3)
        available_space_gb = available_space / (1024**3)
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


def change_ownership(path_to_change, user, group, remote_config=None, dry_run=False, ssh_connection_pools=None):
    """
    Changes the ownership of a file or directory, either locally or remotely.
    """
    if not user and not group:
        return

    owner_spec = f"{user or ''}:{group or ''}".strip(':')

    if dry_run:
        logging.info(f"[DRY RUN] Would change ownership of '{path_to_change}' to '{owner_spec}'.")
        return

    if remote_config:
        logging.info(f"Attempting to change remote ownership of '{path_to_change}' to '{owner_spec}'...")
        pool = ssh_connection_pools.get('DESTINATION_SERVER')
        try:
            with pool.get_connection() as (sftp, ssh):
                escaped_path = path_to_change.replace("'", "'\\''")
                remote_command = f"chown -R -- '{owner_spec}' '{escaped_path}'"
                logging.debug(f"Executing remote command: {remote_command}")
                stdin, stdout, stderr = ssh.exec_command(remote_command, timeout=120)
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


def analyze_torrent(torrent, sftp_config, transfer_mode, ui, pool=None):
    """
    Analyzes a single torrent to determine its size.
    """
    name, hash = torrent.name, torrent.hash
    remote_content_path = torrent.content_path
    logging.debug(f"Analyzing torrent: {name}")
    total_size = None

    try:
        if transfer_mode == 'rsync':
            total_size = get_remote_size_rsync(sftp_config, remote_content_path)
        else:
            with pool.get_connection() as (sftp, ssh):
                total_size = _get_remote_size_du_core(ssh, remote_content_path)

        if total_size is not None and total_size > 0:
            ui.update_torrent_status_text(hash, "Analyzed", color="green")
        elif total_size == 0:
            ui.update_torrent_status_text(hash, "Skipped (zero size)", color="dim")

    except Exception as e:
        ui.log(f"[bold red]Error calculating size for '{name}': {e}[/]")
        ui.update_torrent_status_text(hash, "Analysis Failed", color="bold red")
        return torrent, None
    finally:
        ui.advance_analysis_progress()

    return torrent, total_size


def transfer_torrent(torrent, total_size, source_qbit, destination_qbit, config, tracker_rules, ui, ssh_connection_pools, dry_run=False, test_run=False):
    """
    Executes the transfer and management process for a single, pre-analyzed torrent.
    """
    name, hash = torrent.name, torrent.hash
    source_sftp, source_ssh = None, None
    source_paused = False
    success = False
    all_files = [] # This list will be populated for multi-file torrents
    try:
        # --- Pre-transfer setup and file listing ---
        source_pool = ssh_connection_pools.get('SOURCE_SERVER')
        transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
        source_content_path = torrent.content_path.rstrip('/\\')
        dest_base_path = config['DESTINATION_PATHS']['destination_path']
        content_name = os.path.basename(source_content_path)
        dest_content_path = os.path.join(dest_base_path, content_name)

        if transfer_mode != 'rsync':
            # For SFTP modes, we need to get a list of all files to be transferred
            # to know the total file count for the UI.
            with source_pool.get_connection() as (sftp, ssh):
                source_stat = sftp.stat(source_content_path)
                if source_stat.st_mode & 0o40000:  # S_ISDIR
                    _get_all_files_recursive(sftp, source_content_path, dest_content_path, all_files)
                else: # It's a single file
                    all_files.append((source_content_path, dest_content_path))
        else:
             # For rsync, we treat it as a single file operation for progress tracking
            all_files.append((source_content_path, dest_content_path))

        # Pass the list of file paths (source_f) to the UI manager
        local_cache_sftp_upload = config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
        transfer_multiplier = 2 if transfer_mode == 'sftp_upload' and local_cache_sftp_upload else 1
        ui.start_torrent_transfer(hash, total_size, [source_f for source_f, dest_f in all_files], transfer_multiplier)

        if dry_run:
            logging.info(f"[DRY RUN] Simulating transfer for '{name}'.")
            # Instantly complete the progress bars
            ui.update_torrent_byte_progress(hash, total_size)
            ui.advance_overall_progress(total_size)
            success = True
            return True

        # --- Execute Transfer ---
        remote_dest_base_path = config['DESTINATION_PATHS'].get('remote_destination_path') or dest_base_path
        destination_save_path = remote_dest_base_path

        if transfer_mode == 'rsync':
            transfer_content_rsync(config['SOURCE_SERVER'], source_content_path, dest_content_path, hash, ui, dry_run)
            logging.info(f"TRANSFER: Rsync transfer completed for '{name}'.")
        else:
            max_concurrent_downloads = config['SETTINGS'].getint('max_concurrent_downloads', 4)
            max_concurrent_uploads = config['SETTINGS'].getint('max_concurrent_uploads', 4)
            source_pool = ssh_connection_pools.get('SOURCE_SERVER')

            if transfer_mode == 'sftp_upload':
                logging.info(f"TRANSFER: Starting SFTP-to-SFTP upload for '{name}'...")
                dest_pool = ssh_connection_pools.get('DESTINATION_SERVER')
                local_cache_sftp_upload = config['SETTINGS'].getboolean('local_cache_sftp_upload', False)
                with source_pool.get_connection() as (sftp, ssh):
                    transfer_content_sftp_upload(
                        source_pool, dest_pool, sftp, source_content_path, dest_content_path,
                        hash, ui, max_concurrent_downloads, max_concurrent_uploads, dry_run,
                        local_cache_sftp_upload
                    )
                logging.info(f"TRANSFER: SFTP-to-SFTP upload completed for '{name}'.")
            else: # Default 'sftp' download
                logging.info(f"TRANSFER: Starting SFTP download for '{name}'...")
                with source_pool.get_connection() as (sftp, ssh):
                    transfer_content(source_pool, sftp, source_content_path, dest_content_path, hash, ui, max_concurrent_downloads, dry_run)
                logging.info(f"TRANSFER: SFTP download completed for '{name}'.")


        # --- Post-transfer actions ---
        chown_user = config['SETTINGS'].get('chown_user', '').strip()
        chown_group = config['SETTINGS'].get('chown_group', '').strip()
        if chown_user or chown_group:
            remote_config = config['DESTINATION_SERVER'] if transfer_mode == 'sftp_upload' else None
            change_ownership(dest_content_path, chown_user, chown_group, remote_config, dry_run, ssh_connection_pools)

        destination_save_path = destination_save_path.replace("\\", "/")

        if not dry_run:
            logging.debug(f"Exporting .torrent file for {name}")
            torrent_file_content = source_qbit.torrents_export(torrent_hash=hash)
            logging.info(f"CLIENT: Adding torrent to Destination (paused) with save path '{destination_save_path}': {name}")
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
            logging.info(f"CLIENT: Triggering force recheck on Destination for: {name}")
            destination_qbit.torrents_recheck(torrent_hashes=hash)
        else:
            logging.info(f"[DRY RUN] Would trigger force recheck on Destination for: {name}")

        if wait_for_recheck_completion(destination_qbit, hash, dry_run=dry_run):
            if not dry_run:
                logging.info(f"CLIENT: Starting torrent on Destination: {name}")
                destination_qbit.torrents_resume(torrent_hashes=hash)
            else:
                logging.info(f"[DRY RUN] Would start torrent on Destination: {name}")

            logging.info(f"CLIENT: Attempting to categorize torrent on Destination: {name}")
            set_category_based_on_tracker(destination_qbit, hash, tracker_rules, dry_run=dry_run)

            if not dry_run and not test_run:
                logging.info(f"CLIENT: Pausing torrent on Source before deletion: {name}")
                source_qbit.torrents_pause(torrent_hashes=hash)
                source_paused = True
            else:
                logging.info(f"[DRY RUN/TEST RUN] Would pause torrent on Source: {name}")

            if test_run:
                logging.info(f"[TEST RUN] Skipping deletion of torrent from Source: {name}")
            elif not dry_run:
                logging.info(f"CLIENT: Deleting torrent and data from Source: {name}")
                source_qbit.torrents_delete(torrent_hashes=hash, delete_files=True)
            else:
                logging.info(f"[DRY RUN] Would delete torrent and data from Source: {name}")

            logging.info(f"SUCCESS: Successfully processed torrent: {name}")
            success = True
            return True
        else:
            logging.error(f"Failed to verify recheck for {name}. Leaving on Source for next run.")
            return False
    except Exception as e:
        logging.error(f"An error occurred while processing torrent {name}: {e}", exc_info=True)
        success = False
        return False
    finally:
        ui.stop_torrent_transfer(hash, success=success)


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

    # The RichHandler is now used for non-interactive output.
    # The UIManager will handle the live display.
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

def pid_exists(pid):
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

def test_path_permissions(path_to_test, remote_config=None, ssh_connection_pools=None):
    """
    Tests write permissions for a given path by creating and deleting a temporary file.
    """
    if remote_config:
        logging.info(f"--- Running REMOTE Permission Test on: {path_to_test} ---")
        pool = ssh_connection_pools.get('DESTINATION_SERVER')
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
            logging.error(f"[bold red]FAILURE:[/] Permission denied on remote server when trying to write to '{path_to_test}'.")
            logging.error(f"Please ensure the user '{remote_config['username']}' has write access to this path on the remote server.")
            return False
        except Exception as e:
            logging.error(f"[bold red]FAILURE:[/] An unexpected error occurred during remote permission test: {e}", exc_info=True)
            return False
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
    lock = None  # Initialize lock variable

    try:
        lock = LockFile(script_dir / 'torrent_mover.lock')
        lock.acquire()
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

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

    config_template_path = script_dir / 'config.ini.template'
    update_config(args.config, str(config_template_path))

    ssh_connection_pools = {}
    try:
        config = load_config(args.config)
        # Initialize pools now as they might be needed by utility commands
        server_sections = [s for s in config.sections() if s.endswith('_SERVER')]
        for section_name in server_sections:
            max_sessions = config[section_name].getint('max_concurrent_ssh_sessions', 8)
            ssh_connection_pools[section_name] = SSHConnectionPool(
                host=config[section_name]['host'],
                port=config[section_name].getint('port'),
                username=config[section_name]['username'],
                password=config[section_name]['password'],
                max_size=max_sessions
            )
            logging.info(f"Initialized SSH connection pool for '{section_name}' with size {max_sessions}.")

        if args.list_rules or args.add_rule or args.delete_rule or args.interactive_categorize or args.test_permissions:
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
                test_path_permissions(dest_path, remote_config=remote_config, ssh_connection_pools=ssh_connection_pools)
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
                    if args.category:
                        cat_to_scan = args.category
                        logging.info(f"Using category from command line: '{cat_to_scan}'")
                    else:
                        cat_to_scan = config['SETTINGS'].get('category_to_move')
                        if cat_to_scan:
                            logging.info(f"Using 'category_to_move' from config: '{cat_to_scan}'")
                        else:
                            logging.warning("No category specified via --category or in config. Defaulting to 'uncategorized'.")
                            cat_to_scan = ''
                    run_interactive_categorization(destination_qbit, tracker_rules, script_dir, cat_to_scan, args.no_rules)
                except Exception as e:
                    logging.error(f"Failed to run interactive categorization: {e}", exc_info=True)
                return 0
            return 0

        # --- Main execution block ---
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
            return 1
        except Exception as e:
            logging.error(f"Failed to connect to qBittorrent client after multiple retries: {e}", exc_info=True)
            return 1

        cleanup_orphaned_cache(destination_qbit)
        recovered_torrents = recover_cached_torrents(source_qbit, destination_qbit)
        tracker_rules = load_tracker_rules(script_dir)
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
            logging.info("No torrents to move at this time.")
            return 0

        # This is the main UI-driven execution block.
        total_count = len(eligible_torrents)
        processed_count = 0
        with UIManager() as ui:
            ui.set_analysis_total(total_count)
            ui.update_header(f"Found {total_count} torrents to process. Analyzing...")

            sftp_config = config['SOURCE_SERVER']
            transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()
            analysis_workers = max(10, args.parallel_jobs * 2)
            analyzed_torrents = []
            total_transfer_size = 0

            logging.info("STATE: Starting analysis phase...")
            try:
                for t in eligible_torrents:
                    ui.add_torrent_to_plan(t.name, t.hash, "[dim]Calculating...[/dim]")

                with ThreadPoolExecutor(max_workers=analysis_workers, thread_name_prefix='Analyzer') as executor:
                    source_server_section = 'SOURCE_SERVER'
                    source_pool = ssh_connection_pools.get(source_server_section)
                    if not source_pool:
                        ui.log(f"[bold red]Error: SSH connection pool for server section '{source_server_section}' not found. Check config.[/]")
                        return 1

                    future_to_torrent = {executor.submit(analyze_torrent, t, sftp_config, transfer_mode, ui, pool=source_pool): t for t in eligible_torrents}
                    for future in as_completed(future_to_torrent):
                        _analyzed_torrent, size = future.result()
                        if size is not None and size > 0:
                            analyzed_torrents.append((_analyzed_torrent, size))
                            total_transfer_size += size
                            size_gb = size / (1024**3)
                            ui._torrents_data[_analyzed_torrent.hash]["size"] = f"{size_gb:.2f} GB"
                            ui._update_torrents_table() # Redraw table to show size
            except Exception as e:
                ui.log(f"[bold red]A critical error occurred during the analysis phase: {e}[/]")
                return 1

            logging.info("STATE: Analysis complete.")
            ui.update_header("Analysis complete. Verifying destination...")

            if not analyzed_torrents:
                ui.update_footer("No valid, non-zero size torrents to transfer.")
                logging.info("No valid, non-zero size torrents to transfer.")
                time.sleep(2) # Give user time to see the message
                return 0

            ui.set_overall_total(total_transfer_size)
            if not args.dry_run and not destination_health_check(config, total_transfer_size, ssh_connection_pools):
                ui.update_header("[bold red]Destination health check failed. Aborting transfer process.[/]")
                logging.error("FATAL: Destination health check failed.")
                time.sleep(5)
                return 1

            logging.info("STATE: Starting transfer phase...")
            ui.update_header(f"Transferring {len(analyzed_torrents)} torrents...")
            ui.update_footer("Executing transfers...")

            try:
                with ThreadPoolExecutor(max_workers=args.parallel_jobs, thread_name_prefix='Transfer') as executor:
                    transfer_futures = {
                        executor.submit(
                            transfer_torrent, t, size, source_qbit, destination_qbit, config, tracker_rules,
                            ui, ssh_connection_pools, args.dry_run, args.test_run
                        ): t for t, size in analyzed_torrents
                    }

                    for future in as_completed(transfer_futures):
                        torrent = transfer_futures[future]
                        try:
                            if future.result():
                                processed_count += 1
                        except Exception as e:
                            logging.error(f"An exception was thrown for torrent '{torrent.name}': {e}", exc_info=True)
                            ui.update_torrent_status_text(torrent.hash, "Failed", color="bold red")
                        finally:
                            ui.advance_transfer_progress()
            except KeyboardInterrupt:
                ui.update_header("[bold yellow]Process interrupted by user. Transfers cancelled.[/]")
                ui.update_footer("Shutdown requested.")
                raise

            ui.update_header(f"Processing complete. Moved {processed_count}/{total_count} torrent(s).")
            ui.update_footer("All tasks finished.")
            logging.info(f"Processing complete. Successfully moved {processed_count}/{total_count} torrent(s).")

    except KeyboardInterrupt:
        logging.warning("Process interrupted by user. Shutting down.")
    except KeyError as e:
        logging.error(f"Configuration key missing: {e}. Please check your config.ini.")
        return 1
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)
        return 1
    finally:
        if 'ssh_connection_pools' in locals():
            for pool in ssh_connection_pools.values():
                pool.close_all()
            logging.info("All SSH connections have been closed.")
        # The LockFile's atexit handler will manage cleanup.
        logging.info("--- Torrent Mover script finished ---")
    return 0
if __name__ == "__main__":
    sys.exit(main())