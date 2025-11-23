import configparser
import logging
import time
import os
from typing import List, Optional, Any

import qbittorrentapi
from clients.base import TorrentClient
from utils import retry

# --- Constants ---
class Timeouts:
    SSH_CONNECT = int(os.getenv('TM_SSH_CONNECT_TIMEOUT', '10'))

MAX_RETRY_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 5
GB_BYTES = 1024**3

class QBittorrentClient(TorrentClient):
    """
    qBittorrent implementation of the TorrentClient interface.
    Migrates logic from qbittorrent_manager.py.
    """

    def __init__(self, config_section: configparser.SectionProxy, client_name: str):
        """
        Initialize the QBittorrentClient.

        Args:
            config_section: The configuration section proxy from ConfigParser
                containing connection details (host, port, username, password).
            client_name: A descriptive name for the client (e.g., "Source")
                for logging purposes.
        """
        self.config_section = config_section
        self.client_name = client_name
        self.client: Optional[qbittorrentapi.Client] = None

    @retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
    def connect(self) -> bool:
        """
        Connects to the qBittorrent client with retry logic.

        Returns:
            bool: True if connection was successful, raises exception otherwise (due to retry decorator).
        """
        host = self.config_section['host']
        port = self.config_section.getint('port')
        username = self.config_section['username']
        password = self.config_section['password']
        verify_cert = self.config_section.getboolean('verify_cert', fallback=True)

        logging.info(f"STATE: Connecting to {self.client_name} qBittorrent at {host}...")

        self.client = qbittorrentapi.Client(
            host=host,
            port=port,
            username=username,
            password=password,
            VERIFY_WEBUI_CERTIFICATE=verify_cert,
            REQUESTS_ARGS={'timeout': Timeouts.SSH_CONNECT}
        )

        self.client.auth_log_in()
        logging.info(f"CLIENT: Successfully connected to {self.client_name}. Version: {self.client.app.version}")
        return True

    def get_eligible_torrents(self, category: str, size_threshold_gb: Optional[float] = None) -> List[Any]:
        """
        Retrieves a list of torrents eligible for transfer.
        Adapts logic from qbittorrent_manager.get_eligible_torrents.
        """
        if self.client is None:
            logging.error("Client not connected.")
            return []

        try:
            if size_threshold_gb is None:
                torrents = self.client.torrents_info(category=category, status_filter='completed')
                logging.info(f"Found {len(torrents)} completed torrent(s) in category '{category}' to move.")
                return torrents

            logging.info(f"Size threshold of {size_threshold_gb} GB is active for category '{category}'.")
            all_torrents_in_category = self.client.torrents_info(category=category)
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

    def get_torrent_info(self, torrent_hash: str) -> Optional[Any]:
        if self.client is None:
            return None
        try:
            torrents = self.client.torrents_info(torrent_hashes=torrent_hash)
            return torrents[0] if torrents else None
        except Exception as e:
            logging.error(f"Error getting torrent info for {torrent_hash}: {e}")
            return None

    def pause_torrent(self, torrent_hash: str) -> None:
        if self.client:
            self.client.torrents_pause(torrent_hashes=torrent_hash)

    def resume_torrent(self, torrent_hash: str) -> None:
        if self.client:
            self.client.torrents_resume(torrent_hashes=torrent_hash)

    def delete_torrent(self, torrent_hash: str, delete_files: bool = False) -> None:
        if self.client:
            self.client.torrents_delete(torrent_hashes=torrent_hash, delete_files=delete_files)

    def recheck_torrent(self, torrent_hash: str) -> None:
        if self.client:
            self.client.torrents_recheck(torrent_hashes=torrent_hash)

    def set_category(self, torrent_hash: str, category: str) -> None:
        if self.client:
            self.client.torrents_set_category(torrent_hashes=torrent_hash, category=category)

    def add_torrent(self, torrent_files: bytes, save_path: str, category: str, is_paused: bool = False) -> None:
        """
        Adds a torrent to the client.
        Maps 'add' to 'torrents_add'.
        """
        if self.client:
            self.client.torrents_add(
                torrent_files=torrent_files,
                save_path=save_path,
                category=category,
                is_paused=is_paused
            )

    def export_torrent(self, torrent_hash: str) -> bytes:
        if self.client:
            return self.client.torrents_export(torrent_hash=torrent_hash)
        return b""

    def get_incomplete_files(self, torrent_hash: str) -> List[str]:
        """
        Gets a list of incomplete files for a given torrent.
        Adapts logic from qbittorrent_manager.get_incomplete_files.
        """
        if self.client is None:
            return []
        try:
            files = self.client.torrents_files(torrent_hash=torrent_hash)
            incomplete_files = [f.name for f in files if f.progress < 1]
            if incomplete_files:
                logging.warning(f"Found {len(incomplete_files)} incomplete files for torrent {torrent_hash[:10]}...")
            return incomplete_files
        except Exception as e:
            logging.error(f"Could not get file list for torrent {torrent_hash[:10]}: {e}")
            return []

    def wait_for_recheck(self, torrent_hash: str, ui_manager: Any, stuck_timeout: int, stopped_timeout: int, grace_period: int = 0, dry_run: bool = False) -> str:
        """
        Waits for recheck to complete using Intelligent Patience logic.

        State Machine:
          1. Grace Period: If checking/queued and progress is 0%, ignore stuck timer until grace_period elapsed.
          2. Stuck Detection: If progress > 0% but stops increasing for stuck_timeout, FAILED_STUCK.
          3. Stopped Confirmation: If stopped/error, wait stopped_timeout. Returns FAILED_STATE or FAILED_FINAL_REVIEW.
          4. Success: 100% progress.

        Args:
            torrent_hash: The hash of the torrent to monitor.
            ui_manager: UI Manager to keep watchdog alive.
            stuck_timeout: Seconds to wait if progress stalls.
            stopped_timeout: Seconds to wait if state becomes stopped/error.
            grace_period: Seconds to allow 0% progress at start.
            dry_run: If true, returns SUCCESS immediately.

        Returns:
            str: "SUCCESS", "FAILED_STUCK", "FAILED_STATE", "FAILED_FINAL_REVIEW"
        """
        if dry_run:
            logging.info(f"[DRY RUN] Would wait for recheck on {torrent_hash[:10]}. Assuming success.")
            return "SUCCESS"

        if self.client is None:
            logging.error("Client not connected.")
            return "FAILED_STATE"

        logging.info(f"Waiting for recheck to complete for torrent {torrent_hash[:10]}...")

        # Timer tracking
        start_time = time.time()
        last_progress_increase_time = time.time()
        last_known_progress = -1.0
        stopped_state_detected_time = None
        last_stopped_progress = -1.0

        # API Safety counters
        consecutive_not_found_count = 0

        # States considered "active checking"
        checking_states = ['checkingUP', 'checkingDL', 'queuedUP', 'queuedDL']
        # States considered "stopped/error"
        stopped_states = ['error', 'missingFiles', 'stopped', 'pausedUP', 'pausedDL', 'stoppedUP', 'stoppedDL']

        while True:
            # Ensure watchdog is pet
            if hasattr(ui_manager, 'pet_watchdog'):
                ui_manager.pet_watchdog()

            try:
                torrent_info = self.client.torrents_info(torrent_hashes=torrent_hash)

                # --- API Safety: Handle Disappearance ---
                if not torrent_info:
                    consecutive_not_found_count += 1
                    logging.warning(f"Torrent {torrent_hash[:10]} not returned by API (Attempt {consecutive_not_found_count}).")
                    if consecutive_not_found_count >= 3:
                        logging.error(f"Torrent {torrent_hash[:10]} disappeared permanently after {consecutive_not_found_count} checks.")
                        return "FAILED_STATE"
                    time.sleep(2)
                    continue

                # Reset disappeared counter if found
                consecutive_not_found_count = 0

                torrent = torrent_info[0]
                state = torrent.state
                current_progress = torrent.progress

                # --- Success Condition ---
                if current_progress >= 1.0:
                    logging.info(f"Recheck completed for torrent {torrent_hash[:10]} at 100%.")
                    return "SUCCESS"

                # --- State Handling ---
                if state in checking_states:
                    stopped_state_detected_time = None # Reset stopped timer

                    if current_progress > last_known_progress:
                        # Progress is being made, reset the "stuck" timer
                        last_progress_increase_time = time.time()
                        last_known_progress = current_progress
                        logging.debug(f"Recheck progress: {current_progress*100:.2f}% (State: {state})")
                    else:
                        # No progress. Check if we are "stuck".
                        elapsed_stuck_time = time.time() - last_progress_increase_time
                        elapsed_since_start = time.time() - start_time

                        # Grace Period Logic
                        if current_progress == 0.0 and elapsed_since_start < grace_period:
                            # Reset stuck timer while in grace period
                            last_progress_increase_time = time.time()
                            logging.debug(f"Torrent initializing... (Grace Period: {elapsed_since_start:.1f}/{grace_period}s)")
                        elif elapsed_stuck_time > stuck_timeout:
                            logging.error(f"Recheck FAILED for {torrent_hash[:10]}: No progress for {stuck_timeout} seconds (State: {state}).")
                            return "FAILED_STUCK"

                elif state in stopped_states:
                    # This is the "Stopped" or "Explicit Failure" state
                    if stopped_state_detected_time is None:
                        # First time seeing this state, start the timer
                        logging.warning(f"Recheck for {torrent_hash[:10]} entered a stopped state: '{state}'. Waiting {stopped_timeout}s to confirm.")
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
                        if elapsed_stopped_time > stopped_timeout:
                            logging.error(f"Recheck FAILED for torrent {torrent_hash[:10]}. State confirmed as '{state}' for {stopped_timeout}s.")
                            if current_progress < 1.0:
                                return "FAILED_STATE"
                            return "FAILED_FINAL_REVIEW" # Stopped at 100% but not "Completed" state

                else:
                    # Any other state (e.g., 'uploading', 'stalledUP') means recheck is done
                    # but it didn't hit 100%. This is a failure.
                    logging.warning(f"Recheck for {torrent_hash[:10]} ended prematurely in state '{state}' at {current_progress*100:.2f}%.")
                    if current_progress >= 1.0:
                        return "SUCCESS" # It might be 100% and just changed state
                    else:
                        return "FAILED_STATE"

                time.sleep(2) # Poll every 2 seconds

            except Exception as e:
                logging.error(f"Error while waiting for recheck on {torrent_hash[:10]}: {e}", exc_info=True)
                return "FAILED_STATE"
