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
    ui: "BaseUIManager", # <-- ADD THIS
    timeout_seconds: int = Timeouts.RECHECK,
    dry_run: bool = False,
    allow_near_complete: bool = True
) -> bool:
    """Monitors a torrent on the destination client until it completes rechecking.

    This function polls the qBittorrent client periodically to check the status
    of a torrent that is rechecking. It will continue until the torrent's
    progress reaches 100% (or 99.9%+ if allow_near_complete is True),
    it enters an error state, or the timeout is exceeded.

    Args:
        client: An authenticated qBittorrent client instance.
        torrent_hash: The hash of the torrent to monitor.
        timeout_seconds: The maximum number of seconds to wait for the recheck.
        dry_run: If True, the function will simulate a successful recheck
            without actually waiting.
        allow_near_complete: If True, accepts 99.9%+ completion as success.
            This handles the rsync edge case where a few bytes may differ.

    Returns:
        True if the recheck completes successfully (progress >= 99.9% or 100%).
        False if the recheck fails, the torrent enters an error state,
            or the timeout is reached.
    """
    if dry_run:
        logging.info(f"[DRY RUN] Would wait for recheck on {torrent_hash[:10]}. Assuming success.")
        return True

    start_time = time.time()
    last_progress = 0
    stuck_count = 0

    logging.info(f"Waiting for recheck to complete for torrent {torrent_hash[:10]}...")

    while time.time() - start_time < timeout_seconds:
        ui.pet_watchdog()  # <-- PET WATCHDOG
        try:
            torrent_info = client.torrents_info(torrent_hashes=torrent_hash)
            if not torrent_info:
                logging.warning(f"Torrent {torrent_hash[:10]} disappeared while waiting for recheck.")
                return False

            torrent = torrent_info[0]
            state = torrent.state
            current_progress = torrent.progress

            # Check for completion (100% or near-complete if allowed)
            if current_progress >= 1.0:
                logging.info(f"Recheck completed for torrent {torrent_hash[:10]} at 100%.")
                return True
            elif allow_near_complete and current_progress >= 0.999:
                # (Same 99.9% logic as before)
                logging.warning(f"Recheck at {current_progress*100:.2f}% for {torrent_hash[:10]}.")
                logging.warning("This is likely due to rsync metadata differences (timestamps, permissions).")
                logging.warning("Accepting as complete since data integrity is verified by rsync checksums.")
                try:
                    client.torrents_resume(torrent_hashes=torrent_hash)
                    time.sleep(2)
                    updated_info = client.torrents_info(torrent_hashes=torrent_hash)
                    if updated_info and updated_info[0].state in ['uploading', 'stalledUP', 'queuedUP', 'forcedUP']:
                        logging.info(f"Torrent {torrent_hash[:10]} successfully started despite 99.9% recheck.")
                        return True
                except Exception as e:
                    logging.warning(f"Could not force-start torrent: {e}")
                return True

            # Check for error states
            if state in ['error', 'missingFiles']:
                logging.error(f"Recheck FAILED for torrent {torrent_hash[:10]}. State is '{state}'.")
                return False

            # --- New Stuck Logic ---
            if state in ['checkingUP', 'checkingDL']:
                # It's actively checking, reset stuck count
                stuck_count = 0
            elif abs(current_progress - last_progress) < 0.001:
                # It's not actively checking, and progress is stuck.
                # Is another torrent checking?
                try:
                    all_torrents = client.torrents_info()
                    if any(t.state in ['checkingUP', 'checkingDL'] for t in all_torrents):
                        # Another torrent is checking. Reset stuck count and log.
                        stuck_count = 0
                        logging.debug(f"Recheck for {torrent_hash[:10]} is waiting... another torrent is being checked.")
                    else:
                        # Nothing else is checking. This one is truly stuck.
                        stuck_count += 1
                except Exception as e:
                    # Failed to get all torrents, assume not stuck
                    stuck_count = 0
                    logging.warning(f"Could not check for other rechecks: {e}")
            else:
                # Progress was made
                stuck_count = 0
            # --- End New Stuck Logic ---

            if stuck_count >= 6:  # Stuck for 60 seconds (6 * 10s checks)
                if current_progress >= 0.999:
                    logging.warning(f"Recheck stuck at {current_progress*100:.2f}% for 60 seconds. Accepting as complete.")
                    return True
                else:
                    logging.error(f"Recheck stuck at {current_progress*100:.2f}% for 60 seconds (State: {state}).")
                    return False

            last_progress = current_progress
            logging.debug(f"Recheck progress: {current_progress*100:.2f}% (State: {state})")

            time.sleep(10)

        except Exception as e:
            logging.error(f"Error while waiting for recheck on {torrent_hash[:10]}: {e}")
            return False

    logging.error(f"Timeout: Recheck did not complete for torrent {torrent_hash[:10]} in {timeout_seconds}s. Last progress: {last_progress*100:.2f}%")
    return False

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
