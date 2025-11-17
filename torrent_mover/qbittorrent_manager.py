import configparser
import logging
import time
import os
from typing import List, Optional

import qbittorrentapi

from .utils import retry

# --- Constants ---
# These are moved from torrent_mover.py to support the functions in this module.
class Timeouts:
    SSH_CONNECT = int(os.getenv('TM_SSH_CONNECT_TIMEOUT', '10'))
    RECHECK = int(os.getenv('TM_RECHECK_TIMEOUT', '900'))

MAX_RETRY_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 5
GB_BYTES = 1024**3


@retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
def connect_qbit(config_section: configparser.SectionProxy, client_name: str) -> qbittorrentapi.Client:
    """Connects to a qBittorrent client with retry logic.

    Args:
        config_section: The configuration section proxy from ConfigParser
            containing connection details (host, port, username, password).
        client_name: A descriptive name for the client (e.g., "Source")
            for logging purposes.

    Returns:
        A connected and authenticated qbittorrentapi.Client object.

    Raises:
        qbittorrentapi.exceptions.LoginFailed: If authentication fails.
        requests.exceptions.RequestException: If a network error occurs.
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
        REQUESTS_ARGS={'timeout': Timeouts.SSH_CONNECT}
    )
    client.auth_log_in()
    logging.info(f"CLIENT: Successfully connected to {client_name}. Version: {client.app.version}")
    return client

def get_eligible_torrents(client: qbittorrentapi.Client, category: str, size_threshold_gb: Optional[float] = None) -> List[qbittorrentapi.TorrentDictionary]:
    """Retrieves a list of torrents eligible for transfer.

    This function can operate in two modes:
    1.  If `size_threshold_gb` is None, it fetches all completed torrents
        in the specified category.
    2.  If `size_threshold_gb` is set, it fetches completed torrents sorted
        by added date until the total size of torrents in the category is
        below the threshold.

    Args:
        client: An authenticated qBittorrent client instance.
        category: The category to scan for torrents.
        size_threshold_gb: If provided, the desired maximum size of the
            category in gigabytes. Torrents will be selected for moving
            until the category size is at or below this threshold.

    Returns:
        A list of TorrentDictionary objects that are eligible to be moved.
        Returns an empty list if no torrents meet the criteria or an error occurs.
    """
    try:
        if size_threshold_gb is None:
            torrents = client.torrents_info(category=category, status_filter='completed')
            logging.info(f"Found {len(torrents)} completed torrent(s) in category '{category}' to move.")
            return torrents
        logging.info(f"Size threshold of {size_threshold_gb} GB is active for category '{category}'.")
        all_torrents_in_category = client.torrents_info(category=category)
        current_total_size = sum(t.size for t in all_torrents_in_category)
        threshold_bytes = size_threshold_gb * GB_BYTES
        logging.info(f"Current category size: {current_total_size / GB_BYTES:.2f} GB. Target size: {size_threshold_gb:.2f} GB.")
        if current_total_size <= threshold_bytes:
            logging.info("Category size is already below the threshold. No torrents to move.")
            return []
        size_to_move = current_total_size - threshold_bytes
        logging.info(f"Need to move at least {size_to_move / GB_BYTES:.2f} GB of torrents.")
        completed_torrents = [t for t in all_torrents_in_category if t.progress == 1 and t.state not in ['checkingUP', 'checkingDL']]
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
        logging.info(f"Selected {len(torrents_to_move)} torrent(s) to meet the threshold (Total size: {size_of_selected_torrents / GB_BYTES:.2f} GB).")
        return torrents_to_move
    except Exception as e:
        logging.exception(f"Could not retrieve torrents from client: {e}")
        return []

def wait_for_recheck_completion(
    client: qbittorrentapi.Client,
    torrent_hash: str,
    ui: "BaseUIManager",
    recheck_stuck_timeout: int,
    recheck_stopped_timeout: int,
    dry_run: bool = False
) -> str:
    if dry_run:
        logging.info(f"[DRY RUN] Would wait for recheck on {torrent_hash[:10]}. Assuming success.")
        return "SUCCESS"

    logging.info(f"Waiting for recheck to complete for torrent {torrent_hash[:10]}...")

    # We need to track two separate timeouts
    last_progress_increase_time = time.time()
    last_known_progress = -1.0

    stopped_state_detected_time = None
    last_stopped_progress = -1.0

    while True:
        ui.pet_watchdog()
        try:
            torrent_info = client.torrents_info(torrent_hashes=torrent_hash)
            if not torrent_info:
                logging.error(f"Torrent {torrent_hash[:10]} disappeared while waiting for recheck.")
                return "FAILED_STATE" # Triggers delta-sync

            torrent = torrent_info[0]
            state = torrent.state
            current_progress = torrent.progress

            # --- Success Condition ---
            if current_progress >= 1.0:
                logging.info(f"Recheck completed for torrent {torrent_hash[:10]} at 100%.")
                return "SUCCESS"

            # --- State Handling ---
            if state in ['checkingUP', 'checkingDL']:
                # This is the "Checking" state
                stopped_state_detected_time = None # Reset stopped timer

                if current_progress > last_known_progress:
                    # Progress is being made, reset the "stuck" timer
                    last_progress_increase_time = time.time()
                    last_known_progress = current_progress
                    logging.debug(f"Recheck progress: {current_progress*100:.2f}% (State: {state})")
                else:
                    # No progress. Check if we are "stuck".
                    elapsed_stuck_time = time.time() - last_progress_increase_time
                    if elapsed_stuck_time > recheck_stuck_timeout:
                        logging.error(f"Recheck FAILED for {torrent_hash[:10]}: No progress for {recheck_stuck_timeout} seconds (State: {state}).")
                        return "FAILED_STUCK" # New "Stuck" failure state

            elif state in ['error', 'missingFiles', 'stopped', 'pausedUP', 'pausedDL']:
                # This is the "Stopped" or "Explicit Failure" state
                if stopped_state_detected_time is None:
                    # First time seeing this state, start the timer
                    logging.warning(f"Recheck for {torrent_hash[:10]} entered a stopped state: '{state}'. Waiting {recheck_stopped_timeout}s to confirm.")
                    stopped_state_detected_time = time.time()
                    last_stopped_progress = current_progress

                # Check if progress has changed (e.g., user fixed it)
                if current_progress > last_stopped_progress:
                     logging.info(f"Progress detected on stopped torrent {torrent_hash[:10]}. Resetting timers.")
                     stopped_state_detected_time = None # Reset
                     last_progress_increase_time = time.time() # Reset stuck timer too
                     last_known_progress = current_progress
                else:
                    # No progress. Check if the "stopped" timeout has elapsed.
                    elapsed_stopped_time = time.time() - stopped_state_detected_time
                    if elapsed_stopped_time > recheck_stopped_timeout:
                        logging.error(f"Recheck FAILED for torrent {torrent_hash[:10]}. State confirmed as '{state}' for {recheck_stopped_timeout}s.")
                        return "FAILED_STATE" # Triggers delta-sync

            else:
                # Any other state (e.g., 'uploading', 'stalledUP') means recheck is done
                # but it didn't hit 100%. This is a failure.
                logging.warning(f"Recheck for {torrent_hash[:10]} ended prematurely in state '{state}' at {current_progress*100:.2f}%.")
                if current_progress >= 1.0:
                    return "SUCCESS" # It might be 100% and just changed state
                else:
                    return "FAILED_STATE" # Triggers delta-sync

            time.sleep(2) # Poll every 2 seconds

        except Exception as e:
            logging.error(f"Error while waiting for recheck on {torrent_hash[:10]}: {e}", exc_info=True)
            return "FAILED_STATE" # Triggers delta-sync

def get_incomplete_files(client: qbittorrentapi.Client, torrent_hash: str) -> List[str]:
    """
    Gets a list of incomplete files for a given torrent.

    Args:
        client: The qBittorrent client.
        torrent_hash: The hash of the torrent to check.

    Returns:
        A list of file names (paths relative to torrent root) that are not 100% complete.
    """
    try:
        files = client.torrents_files(torrent_hash=torrent_hash)
        incomplete_files = [f.name for f in files if f.progress < 1]
        if incomplete_files:
            logging.warning(f"Found {len(incomplete_files)} incomplete files for torrent {torrent_hash[:10]}...")
        return incomplete_files
    except Exception as e:
        logging.error(f"Could not get file list for torrent {torrent_hash[:10]}: {e}")
        return []
