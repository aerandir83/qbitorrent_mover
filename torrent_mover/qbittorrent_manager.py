"""Manages interactions with qBittorrent clients.

This module provides a centralized set of functions for connecting to, retrieving
data from, and managing torrents in qBittorrent clients. It includes logic for:
- Connecting to a qBittorrent client with authentication and retry mechanisms.
- Identifying torrents that are eligible for transfer based on category and
  disk space thresholds.
- Monitoring the rechecking process of torrents on a destination client.
- Retrieving the list of incomplete files for a given torrent to facilitate
  delta transfers.
"""
import configparser
import logging
import time
import os
from typing import List, Optional

import qbittorrentapi
from qbittorrentapi.exceptions import LoginFailed
from requests.exceptions import RequestException

from .utils import retry

# --- Constants ---
class Timeouts:
    """Defines timeout constants for network operations, configurable via environment variables."""
    SSH_CONNECT = int(os.getenv('TM_SSH_CONNECT_TIMEOUT', '10'))
    RECHECK = int(os.getenv('TM_RECHECK_TIMEOUT', '900'))

MAX_RETRY_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 5
GB_BYTES = 1024**3


@retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
def connect_qbit(config_section: configparser.SectionProxy, client_name: str) -> qbittorrentapi.Client:
    """Establishes a connection to a qBittorrent client with retry logic.

    This function attempts to connect to and authenticate with a qBittorrent client
    using the provided configuration. If the connection fails, it will retry
    multiple times before raising an exception.

    Args:
        config_section: The `ConfigParser` section proxy containing connection
            details (host, port, username, password, verify_cert).
        client_name: A descriptive name for the client (e.g., "Source") for
            use in log messages.

    Returns:
        A connected and authenticated `qbittorrentapi.Client` object.

    Raises:
        LoginFailed: If authentication with the qBittorrent client fails after
            all retry attempts.
        RequestException: If a network-level error occurs (e.g., DNS failure,
            connection refused) after all retry attempts.
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
    """Retrieves a list of torrents that are eligible for transfer.

    This function identifies torrents to be moved based on two modes of operation:
    1.  Standard Mode (`size_threshold_gb` is `None`): Fetches all torrents that
        have completed downloading and belong to the specified category.
    2.  Threshold Mode (`size_threshold_gb` is set): Calculates the total size of
        all torrents in the category. If the total size exceeds the threshold,
        it selects the oldest completed torrents for moving until the category's
        total size is at or below the threshold.

    Args:
        client: An authenticated `qbittorrentapi.Client` instance.
        category: The category to scan for torrents.
        size_threshold_gb: An optional float. If provided, the function will
            select torrents to move until the category's total size is below
            this value in gigabytes.

    Returns:
        A list of `TorrentDictionary` objects representing the torrents that
        are eligible to be moved. Returns an empty list if no torrents meet
        the criteria or if an error occurs.
    """
    try:
        if size_threshold_gb is None:
            # Standard mode: Get all completed torrents in the category
            torrents = client.torrents_info(category=category, status_filter='completed')
            logging.info(f"Found {len(torrents)} completed torrent(s) in category '{category}' to move.")
            return torrents

        # Threshold mode: Select torrents to meet the size target
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

        # Filter for completed torrents and sort by date added (oldest first)
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
    timeout_seconds: int = Timeouts.RECHECK,
    dry_run: bool = False,
    allow_near_complete: bool = True
) -> bool:
    """Monitors a torrent on the destination client until its recheck is complete.

    This function polls the qBittorrent client periodically to check the status
    of a torrent being rechecked. It handles several conditions:
    - Success: The torrent's progress reaches 100%.
    - Near Success (for rsync): If `allow_near_complete` is `True`, progress of
      99.9% or higher is considered a success to account for minor metadata
      differences that rsync might not replicate perfectly.
    - Failure: The torrent enters an 'error' or 'missingFiles' state.
    - Timeout: The recheck does not complete within the specified `timeout_seconds`.
    - Stuck: The recheck progress does not change for a significant period.

    Args:
        client: An authenticated qBittorrent client instance.
        torrent_hash: The hash of the torrent to monitor.
        timeout_seconds: The maximum number of seconds to wait.
        dry_run: If `True`, simulates a successful recheck without waiting.
        allow_near_complete: If `True`, accepts progress >= 99.9% as complete.

    Returns:
        `True` if the recheck completes successfully.
        `False` if the recheck fails, times out, or the torrent enters an error state.
    """
    if dry_run:
        logging.info(f"[DRY RUN] Would wait for recheck on {torrent_hash[:10]}. Assuming success.")
        return True

    start_time = time.time()
    last_progress = -1.0
    stuck_count = 0

    logging.info(f"Waiting for recheck to complete for torrent {torrent_hash[:10]}...")

    while time.time() - start_time < timeout_seconds:
        try:
            torrent_info = client.torrents_info(torrent_hashes=torrent_hash)
            if not torrent_info:
                logging.warning(f"Torrent {torrent_hash[:10]} disappeared while waiting for recheck.")
                return False

            torrent = torrent_info[0]
            current_progress = torrent.progress

            # Check for 100% completion
            if current_progress >= 1.0:
                logging.info(f"Recheck completed for torrent {torrent_hash[:10]} at 100%.")
                return True

            # Handle near-complete case for rsync
            if allow_near_complete and current_progress >= 0.999:
                logging.warning(f"Recheck at {current_progress*100:.2f}% for {torrent_hash[:10]}.")
                logging.warning("This is likely due to rsync metadata differences. Accepting as complete.")
                # Attempt to force-start the torrent
                try:
                    client.torrents_resume(torrent_hashes=torrent_hash)
                    time.sleep(2)
                    updated_info = client.torrents_info(torrent_hashes=torrent_hash)
                    if updated_info and updated_info[0].state in ['uploading', 'stalledUP', 'queuedUP', 'forcedUP']:
                        logging.info(f"Torrent {torrent_hash[:10]} successfully started despite 99.9% recheck.")
                except Exception as e:
                    logging.warning(f"Could not force-start torrent after near-complete recheck: {e}")
                return True

            # Check for error states
            if torrent.state in ['error', 'missingFiles']:
                logging.error(f"Recheck FAILED for torrent {torrent_hash[:10]}. State is '{torrent.state}'.")
                return False

            # Check if progress is stuck
            if abs(current_progress - last_progress) < 0.001:  # Progress change less than 0.1%
                stuck_count += 1
                if stuck_count >= 6:  # Stuck for ~60 seconds (6 checks * 10s sleep)
                    logging.error(f"Recheck stuck at {current_progress*100:.2f}% for 60 seconds.")
                    return False
            else:
                stuck_count = 0  # Reset counter if progress is made

            last_progress = current_progress
            logging.debug(f"Recheck progress for {torrent.name}: {current_progress*100:.2f}%")
            time.sleep(10)

        except Exception as e:
            logging.error(f"Error while waiting for recheck on {torrent_hash[:10]}: {e}")
            return False

    logging.error(f"Timeout: Recheck did not complete for torrent {torrent_hash[:10]} in {timeout_seconds}s. Last progress: {last_progress*100:.2f}%")
    return False

def get_incomplete_files(client: qbittorrentapi.Client, torrent_hash: str) -> List[str]:
    """Gets a list of incomplete files for a given torrent.

    This function is used to identify which specific files need to be re-downloaded
    or re-transferred when a recheck fails, enabling a more efficient delta
    transfer instead of re-transferring the entire torrent content.

    Args:
        client: The qBittorrent client where the torrent resides.
        torrent_hash: The hash of the torrent to check.

    Returns:
        A list of file paths (relative to the torrent's root content path)
        that are not 100% complete. Returns an empty list if all files are
        complete or if an error occurs.
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
