#!/usr/bin/env python3
# Torrent Mover
#
# A script to automatically move completed torrents from a source client
# to a destination client and transfer the files via SFTP or rsync.

__version__ = "2.0.0"

import sys
import logging
from pathlib import Path
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
import os
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

# --- Project-specific imports ---
from .clients import get_client
from .clients.base import TorrentClient, Torrent

SSH_CONTROL_PATH = None

def setup_ssh_control_path():
    """Creates a directory for the SSH control socket."""
    global SSH_CONTROL_PATH
    try:
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
    setup_ssh_control_path()


def _get_ssh_command(port):
    """Builds the SSH command for rsync, enabling connection multiplexing if available."""
    base_ssh_cmd = f"ssh -p {port} -c chacha20-poly1305@openssh.com,aes128-ctr -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=15"
    if SSH_CONTROL_PATH:
        multiplex_opts = f"-o ControlMaster=auto -o ControlPath='{SSH_CONTROL_PATH}' -o ControlPersist=60s"
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

    if not config_file.is_file():
        logging.warning(f"Configuration file not found at '{config_path}'.")
        logging.warning("Creating a new one from the template. Please review and fill it out.")
        try:
            shutil.copy2(template_file, config_file)
        except OSError as e:
            logging.error(f"FATAL: Could not create config file: {e}")
            sys.exit(1)
        return

    try:
        updater = configupdater.ConfigUpdater()
        updater.read(config_file, encoding='utf-8')
        template_updater = configupdater.ConfigUpdater()
        template_updater.read(template_file, encoding='utf-8')

        changes_made = False
        for section_name in template_updater.sections():
            if not updater.has_section(section_name):
                updater.add_section(section_name)
                # Copy the whole section including comments
                updater[section_name].from_string(str(template_updater[section_name]))
                changes_made = True
                logging.info(f"Added new section to config: [{section_name}]")
            else:
                template_section = template_updater[section_name]
                user_section = updater[section_name]
                for key, opt in template_section.items():
                    if not user_section.has_option(key):
                        # Add the new option with its value and comments
                        new_opt = user_section.set(key, opt.value)
                        if opt.comments.above:
                            new_opt.add_comment('\n'.join(opt.comments.above), above=True)
                        if opt.comments.inline:
                            new_opt.add_comment(opt.comments.inline, inline=True)
                        changes_made = True
                        logging.info(f"Added new option in [{section_name}]: {key}")

        if changes_made:
            backup_dir = config_file.parent / 'backup'
            backup_dir.mkdir(exist_ok=True)
            backup_filename = f"{config_file.stem}.bak_{time.strftime('%Y%m%d-%H%M%S')}"
            backup_path = backup_dir / backup_filename
            shutil.copy2(config_file, backup_path)
            logging.info(f"Backed up existing configuration to '{backup_path}'")
            updater.write_to_file(config_file, encoding='utf-8')
            logging.info("Configuration file has been updated with new options.")
        else:
            logging.info("Configuration file is already up-to-date.")

    except Exception as e:
        logging.error(f"FATAL: An error occurred during config update: {e}", exc_info=True)
        sys.exit(1)


def load_config(config_path="config.ini"):
    """Loads the configuration from the specified .ini file."""
    config_file = Path(config_path)
    if not config_file.is_file():
        logging.error(f"FATAL: Configuration file not found at '{config_path}'.")
        logging.error("Please copy 'config.ini.template' to 'config.ini' and fill in your details.")
        sys.exit(1)

    config = configupdater.ConfigUpdater()
    config.read(config_file)
    return config


def get_remote_size_rsync(sftp_config, remote_path):
    """
    Gets the total size of a remote file or directory using rsync --stats.
    """
    host = sftp_config['remote_host']
    port = sftp_config.getint('remote_port')
    username = sftp_config['remote_user']
    password = sftp_config['remote_password']

    remote_spec = f"{username}@{host}:{remote_path}"

    rsync_cmd = [
        "sshpass", "-p", password,
        "rsync",
        "-r", "--dry-run", "--stats", "--timeout=60", "-W",
        "-e", _get_ssh_command(port),
        remote_spec,
        tempfile.gettempdir() # A dummy local path is required.
    ]

    try:
        result = subprocess.run(
            rsync_cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        if result.returncode != 0 and result.returncode != 24:
            raise Exception(f"Rsync (size check) failed with exit code {result.returncode}. Stderr: {result.stderr}")

        match = re.search(r"Total file size: ([\d,]+) bytes", result.stdout)
        if match:
            size_str = match.group(1).replace(',', '')
            return int(size_str)
        else:
            logging.warning(f"Could not parse rsync --stats output for '{os.path.basename(remote_path)}'.")
            logging.debug(f"Rsync stdout for size check:\n{result.stdout}")
            raise Exception("Failed to parse rsync stats output.")
    except FileNotFoundError:
        logging.error("FATAL: 'rsync' or 'sshpass' command not found during size check.")
        raise
    except Exception as e:
        raise e


def transfer_content_rsync(sftp_config, remote_path, local_path, job_progress, parent_task_id, overall_progress, overall_task_id, dry_run=False):
    """Transfers a remote file or directory to a local path using rsync."""
    host = sftp_config['remote_host']
    port = sftp_config.getint('remote_port')
    username = sftp_config['remote_user']
    password = sftp_config['remote_password']

    local_parent_dir = os.path.dirname(local_path)
    Path(local_parent_dir).mkdir(parents=True, exist_ok=True)
    remote_spec = f"{username}@{host}:{remote_path}"

    rsync_cmd = [
        "sshpass", "-p", password,
        "rsync",
        "-r", "--partial", "--inplace", "--info=progress2", "--timeout=60", "-W",
        "-e", _get_ssh_command(port),
        remote_spec,
        local_parent_dir
    ]
    safe_rsync_cmd = list(rsync_cmd)
    safe_rsync_cmd[2] = "'********'"

    if dry_run:
        logging.info(f"[DRY RUN] Would execute rsync for: {os.path.basename(remote_path)}")
        logging.debug(f"[DRY RUN] Command: {' '.join(safe_rsync_cmd)}")
        task = job_progress.tasks[parent_task_id]
        job_progress.update(parent_task_id, advance=task.total)
        overall_progress.update(overall_task_id, advance=task.total)
        return

    if Path(local_path).exists():
        logging.info(f"Partial file/directory found for '{os.path.basename(remote_path)}'. Resuming with rsync.")
    else:
        logging.info(f"Starting rsync transfer for '{os.path.basename(remote_path)}'")
    logging.debug(f"Executing rsync command: {' '.join(safe_rsync_cmd)}")

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
    stderr_output = process.stderr.read() if process.stderr else ""

    if process.returncode == 0 or process.returncode == 24:
        task = job_progress.tasks[parent_task_id]
        if task.completed < task.total:
            remaining = task.total - task.completed
            if remaining > 0:
                job_progress.update(parent_task_id, advance=remaining)
                overall_progress.update(overall_task_id, advance=remaining)
        logging.info(f"Rsync transfer completed successfully for '{os.path.basename(remote_path)}'.")
    else:
        logging.error(f"Rsync failed for '{os.path.basename(remote_path)}' with exit code {process.returncode}.")
        logging.error(f"Rsync stderr: {stderr_output}")
        raise Exception(f"Rsync transfer failed for {os.path.basename(remote_path)}")


def wait_for_recheck_completion(client: TorrentClient, torrent_hash: str, timeout_seconds=900, dry_run=False):
    if dry_run:
        logging.info(f"[DRY RUN] Would wait for recheck on {torrent_hash[:10]}. Assuming success.")
        return True
    start_time = time.time()
    logging.info(f"Waiting for recheck to complete for torrent {torrent_hash[:10]}...")
    while time.time() - start_time < timeout_seconds:
        try:
            torrent = client.get_torrent_info(torrent_hash)
            if not torrent:
                logging.warning(f"Torrent {torrent_hash[:10]} disappeared while waiting for recheck.")
                return False
            if torrent.progress == 1:
                logging.info(f"Recheck completed for torrent {torrent_hash[:10]}.")
                return True
            time.sleep(10)
        except Exception as e:
            logging.error(f"Error while waiting for recheck on {torrent_hash[:10]}: {e}")
            return False
    logging.error(f"Timeout: Recheck did not complete for torrent {torrent_hash[:10]} in {timeout_seconds}s.")
    return False

def load_tracker_rules(script_dir, rules_filename="tracker_rules.json"):
    rules_file = script_dir / rules_filename
    if not rules_file.is_file():
        return {}
    try:
        with open(rules_file, 'r') as f:
            return json.load(f)
    except json.JSONDecodeError:
        logging.error(f"Could not decode JSON from '{rules_file}'.")
        return {}


def save_tracker_rules(rules, script_dir, rules_filename="tracker_rules.json"):
    rules_file = script_dir / rules_filename
    try:
        with open(rules_file, 'w') as f:
            json.dump(rules, f, indent=4, sort_keys=True)
        return True
    except Exception as e:
        logging.error(f"Failed to save rules to '{rules_file}': {e}")
        return False


def get_tracker_domain(tracker_url):
    try:
        netloc = urlparse(tracker_url).netloc
        parts = netloc.split('.')
        if len(parts) > 2 and parts[0] in ['tracker', 'announce', 'www']:
            return '.'.join(parts[1:])
        return netloc
    except Exception:
        return None

def get_category_from_rules(torrent: Torrent, rules: dict, client: TorrentClient):
    try:
        trackers = client.get_trackers(torrent.hash)
        for tracker in trackers:
            domain = get_tracker_domain(tracker.get('url'))
            if domain and domain in rules:
                return rules[domain]
    except Exception as e:
        logging.warning(f"Could not check trackers for torrent '{torrent.name}': {e}")
    return None

def set_category_based_on_tracker(client: TorrentClient, torrent_hash: str, tracker_rules: dict, dry_run=False):
    try:
        torrent = client.get_torrent_info(torrent_hash)
        if not torrent:
            logging.warning(f"Could not find torrent {torrent_hash[:10]} on destination to categorize.")
            return
        category = get_category_from_rules(torrent, tracker_rules, client)
        if category:
            if category == "ignore":
                logging.info(f"Rule is to ignore torrent '{torrent.name}'. Doing nothing.")
                return
            if torrent.category == category:
                return
            logging.info(f"Rule found. Setting category to '{category}' for '{torrent.name}'.")
            if not dry_run:
                client.set_category(torrent.hash, category)
            else:
                logging.info(f"[DRY RUN] Would set category of '{torrent.name}' to '{category}'.")
    except Exception as e:
        logging.error(f"An error occurred during categorization for torrent {torrent_hash[:10]}: {e}", exc_info=True)


def analyze_torrent(torrent: Torrent, source_config, live_console):
    """Analyzes a single torrent to determine its size."""
    name = torrent.name
    logging.info(f"Analyzing torrent: {name}")

    # Apply source path mapping to get the real path for the transfer tool
    client_path_prefix = source_config.get('source_client_path', '').rstrip('/')
    transfer_path_prefix = source_config.get('source_transfer_path', '').rstrip('/')

    if client_path_prefix and transfer_path_prefix:
        if torrent.content_path.startswith(client_path_prefix):
            remote_content_path = transfer_path_prefix + torrent.content_path[len(client_path_prefix):]
            logging.debug(f"Path mapped: '{torrent.content_path}' -> '{remote_content_path}'")
        else:
            logging.warning(f"Content path '{torrent.content_path}' does not match client_path_prefix '{client_path_prefix}'. Using original path.")
            remote_content_path = torrent.content_path
    else:
        remote_content_path = torrent.content_path

    try:
        total_size = get_remote_size_rsync(source_config, remote_content_path)
    except Exception as e:
        live_console.log(f"[bold red]Error calculating size for '{name}': {e}[/]")
        live_console.log(f"[yellow]Warning: Skipping torrent due to size calculation failure.[/]")
        return torrent, None, None

    if total_size == 0:
        live_console.log(f"[yellow]Warning: Skipping zero-size torrent: {name}[/]")

    return torrent, total_size, remote_content_path


def transfer_torrent(torrent: Torrent, total_size: int, remote_content_path: str, source_client: TorrentClient, dest_client: TorrentClient, source_config, dest_config, tracker_rules, job_progress, overall_progress, overall_task_id, task_add_lock, dry_run=False, test_run=False):
    """Executes the transfer and management process for a single, pre-analyzed torrent."""
    name, hash_ = torrent.name, torrent.hash
    source_paused = False
    parent_task_id = None
    success = False
    try:
        # --- Determine Destination Paths ---
        dest_transfer_base_path = dest_config.get('destination_transfer_path')
        if not dest_transfer_base_path:
            raise ValueError("`destination_transfer_path` is not defined in the destination config.")

        dest_client_base_path = dest_config.get('destination_client_path') or dest_transfer_base_path

        content_name = os.path.basename(remote_content_path)
        local_dest_path = os.path.join(dest_transfer_base_path, content_name)
        client_dest_path = dest_client_base_path.replace("\\", "/")

        with task_add_lock:
            parent_task_id = job_progress.add_task(name, total=total_size, start=True)

        transfer_content_rsync(source_config, remote_content_path, local_dest_path, job_progress, parent_task_id, overall_progress, overall_task_id, dry_run)

        torrent_file_content = None
        torrent_url = None
        if not dry_run:
            logging.info(f"Attempting to get torrent data for {name}")
            torrent_file_content = source_client.export_torrent(hash_)
            if not torrent_file_content:
                logging.warning(f"Could not export .torrent file for '{name}'. Trying to get magnet URI as fallback.")
                torrent_url = source_client.get_torrent_url(hash_)
                if not torrent_url:
                    raise Exception(f"Failed to get both .torrent file and magnet URI for '{name}'. Cannot add to destination.")

        if not dry_run:
            logging.info(f"Adding torrent to Destination (paused) with save path '{client_dest_path}': {name}")
            dest_client.add_torrent(
                torrent_content=torrent_file_content,
                torrent_url=torrent_url,
                save_path=client_dest_path,
                is_paused=True,
                category=torrent.category,
                use_auto_tm=True
            )
            time.sleep(5)
        else:
            logging.info(f"[DRY RUN] Would add torrent to Destination (paused) with save path '{client_dest_path}': {name}")

        if not dry_run:
            logging.info(f"Triggering force recheck on Destination for: {name}")
            dest_client.recheck(hash_)
        else:
            logging.info(f"[DRY RUN] Would trigger force recheck on Destination for: {name}")

        if wait_for_recheck_completion(dest_client, hash_, dry_run=dry_run):
            if not dry_run:
                logging.info(f"Starting torrent on Destination: {name}")
                dest_client.resume(hash_)
            else:
                logging.info(f"[DRY RUN] Would start torrent on Destination: {name}")

            logging.info(f"Attempting to categorize torrent on Destination: {name}")
            set_category_based_on_tracker(dest_client, hash_, tracker_rules, dry_run=dry_run)

            if not dry_run and not test_run:
                logging.info(f"Pausing torrent on Source before deletion: {name}")
                source_client.pause(hash_)
                source_paused = True
            else:
                logging.info(f"[DRY RUN/TEST RUN] Would pause torrent on Source: {name}")

            if test_run:
                logging.info(f"[TEST RUN] Skipping deletion of torrent from Source: {name}")
            elif not dry_run:
                logging.info(f"Deleting torrent and data from Source: {name}")
                source_client.remove_torrent(hash_, delete_files=True)
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
                source_client.resume(hash_)
            except Exception as resume_e:
                logging.error(f"Failed to resume torrent {name} on Source after error: {resume_e}")
        return False
    finally:
        if parent_task_id is not None:
            job_progress.stop_task(parent_task_id)
            if success:
                original_description = job_progress.tasks[parent_task_id].description
                job_progress.update(parent_task_id, description=f"[green]✓[/green] {original_description}")


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
    if pid <= 0: return False
    try:
        os.kill(pid, 0)
    except OSError as err:
        return err.errno == errno.EPERM
    return True

def main():
    """Main entry point for the script."""
    script_dir = Path(__file__).resolve().parent
    lock_file_path = script_dir / 'torrent_mover.lock'

    if lock_file_path.exists():
        try:
            pid = int(lock_file_path.read_text().strip())
            if pid_exists(pid):
                logging.error(f"Script is already running with PID {pid}. Aborting.")
                sys.exit(1)
            else:
                logging.warning(f"Found stale lock file for PID {pid}. Removing it.")
                lock_file_path.unlink()
        except (IOError, ValueError) as e:
            logging.warning(f"Could not read or parse PID from lock file: {e}. Removing stale file.")
            lock_file_path.unlink(missing_ok=True)

    default_config_path = script_dir / 'config.ini'
    parser = argparse.ArgumentParser(description="A script to move torrents and data between servers.", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--config', default=str(default_config_path), help='Path to the configuration file.')
    parser.add_argument('--dry-run', action='store_true', help='Simulate the process without making any changes.')
    parser.add_argument('--test-run', action='store_true', help='Run the full process but do not delete the source torrent.')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging to file.')
    parser.add_argument('--parallel-jobs', type=int, default=4, metavar='N', help='Number of torrents to process in parallel.')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    setup_logging(script_dir, args.dry_run, args.test_run, args.debug)
    config_template_path = script_dir / 'config.ini.template'
    update_config(args.config, str(config_template_path))

    try:
        with open(lock_file_path, 'w') as f: f.write(str(os.getpid()))

        config = load_config(args.config)
        if config['SETTINGS'].get('transfer_mode', 'rsync').lower() == 'rsync':
            check_sshpass_installed()

        try:
            source_section_name = config['SETTINGS']['source'].value
            dest_section_name = config['SETTINGS']['destination'].value
            source_config = config[source_section_name]
            dest_config = config[dest_section_name]

            source_client = get_client(source_config)
            source_client.connect()
            dest_client = get_client(dest_config)
            dest_client.connect()
        except (KeyError, ValueError) as e:
            logging.error(f"Configuration Error: {e}", exc_info=True)
            return 1

        tracker_rules = load_tracker_rules(script_dir)
        category_to_move = config['SETTINGS']['category_to_move'].value
        size_threshold_gb = config['SETTINGS'].getfloat('size_threshold_gb', fallback=None)

        eligible_torrents = source_client.get_torrents_to_move(category_to_move, size_threshold_gb)
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
        overall_progress = Progress(TextColumn("[bold green]Overall"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TotalFileSizeColumn(), TransferSpeedColumn(), TimeRemainingColumn())
        overall_task = overall_progress.add_task("Total progress", total=0)
        task_add_lock = threading.Lock()
        plan_lock = threading.Lock()
        overall_progress_lock = threading.Lock()
        job_progress = Progress(TextColumn("  {task.description}", justify="left"), BarColumn(finished_style="green"), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TotalFileSizeColumn(), TransferSpeedColumn(), TimeRemainingColumn(), TimeElapsedColumn())
        plan_text = Text(f"Found {total_count} torrents to process...\n", style="bold")
        plan_text.append("─" * 70 + "\n", style="dim")
        plan_panel = Panel(plan_text, title="[bold magenta]Transfer Plan[/bold magenta]", border_style="magenta", expand=False)
        layout = Group(plan_panel, Panel(Group(torrent_progress, analysis_progress), title="Overall Queue", border_style="blue"), Panel(overall_progress, title="Total Progress", border_style="green"), Panel(job_progress, title="Active Transfers", border_style="yellow", padding=(1, 2)))

        with Live(layout, refresh_per_second=4, transient=True) as live:
            analysis_workers = max(10, args.parallel_jobs * 2)
            with ThreadPoolExecutor(max_workers=analysis_workers, thread_name_prefix='Analyzer') as analysis_executor, \
                 ThreadPoolExecutor(max_workers=args.parallel_jobs, thread_name_prefix='Transfer') as transfer_executor:

                analysis_future_to_torrent = {
                    analysis_executor.submit(analyze_torrent, torrent, source_config, live.console): torrent
                    for torrent in eligible_torrents
                }
                transfer_future_to_torrent = {}

                for future in as_completed(analysis_future_to_torrent):
                    original_torrent = analysis_future_to_torrent[future]
                    try:
                        analyzed_torrent, total_size, remote_path = future.result()
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
                                transfer_torrent, analyzed_torrent, total_size, remote_path,
                                source_client, dest_client, source_config, dest_config, tracker_rules,
                                job_progress, overall_progress, overall_task, task_add_lock,
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

                for future in as_completed(transfer_future_to_torrent):
                    torrent = transfer_future_to_torrent[future]
                    try:
                        if future.result():
                            processed_count += 1
                    except Exception as e:
                        live.console.log(f"[bold red]An exception was thrown for torrent '{torrent.name}': {e}[/]", exc_info=True)
                    finally:
                        torrent_progress.update(torrent_task, advance=1)

        logging.info(f"Processing complete. Successfully moved {processed_count}/{total_count} torrent(s).")
    except KeyboardInterrupt:
        logging.warning("Process interrupted by user. Shutting down.")
    except Exception as e:
        logging.error(f"An unexpected error occurred in main: {e}", exc_info=True)
        return 1
    finally:
        if lock_file_path.exists():
            try:
                pid = int(lock_file_path.read_text().strip())
                if pid == os.getpid():
                    lock_file_path.unlink()
                    logging.info("Lock file removed.")
            except (IOError, ValueError) as e:
                logging.error(f"Could not read or verify lock file before removing: {e}")
        logging.info("--- Torrent Mover script finished ---")
    return 0

if __name__ == "__main__":
    # To run this script directly for development, you might need to adjust the Python path
    # to include the parent directory, e.g., by running as `python3 -m torrent_mover.torrent_mover`
    # from the root of the repository.
    sys.exit(main())