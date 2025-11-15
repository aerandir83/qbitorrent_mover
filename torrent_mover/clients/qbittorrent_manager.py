import configparser
import logging
import time
import os
from typing import List, Optional, Any

import qbittorrentapi

from ..utils import retry
from .torrent_client import TorrentClientInterface

# --- Constants ---
# These are moved from torrent_mover.py to support the functions in this module.
class Timeouts:
    SSH_CONNECT = int(os.getenv('TM_SSH_CONNECT_TIMEOUT', '10'))
    RECHECK = int(os.getenv('TM_RECHECK_TIMEOUT', '900'))

MAX_RETRY_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 5
GB_BYTES = 1024**3


class QBittorrentClient(TorrentClientInterface):
    """
    An implementation of the TorrentClientInterface for qBittorrent.
    """

    def __init__(self, config_section: configparser.SectionProxy, client_name: str):
        """
        Initializes the QBittorrentClient.

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
    def connect(self):
        """Connects to the qBittorrent client with retry logic.

        Raises:
            qbittorrentapi.exceptions.LoginFailed: If authentication fails.
            requests.exceptions.RequestException: If a network error occurs.
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

    def get_eligible_torrents(self, category: str, size_threshold_gb: Optional[float] = None) -> List[Any]:
        """Retrieves a list of torrents eligible for transfer.

        This function can operate in two modes:
        1.  If `size_threshold_gb` is None, it fetches all completed torrents
            in the specified category.
        2.  If `size_threshold_gb` is set, it fetches completed torrents sorted
            by added date until the total size of torrents in the category is
            below the threshold.

        Args:
            category: The category to scan for torrents.
            size_threshold_gb: If provided, the desired maximum size of the
                category in gigabytes. Torrents will be selected for moving
                until the category size is at or below this threshold.

        Returns:
            A list of TorrentDictionary objects that are eligible to be moved.
            Returns an empty list if no torrents meet the criteria or an error occurs.
        """
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
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

    def export_torrent_file(self, torrent_hash: str) -> bytes:
        """Exports the .torrent file for a given torrent."""
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            return self.client.torrents_export(torrent_hash=torrent_hash)
        except qbittorrentapi.exceptions.NotFound404Error:
            logging.error(f"Cannot export .torrent file for hash {torrent_hash}: torrent not found.")
            raise
        except Exception as e:
            logging.error(f"Failed to export .torrent file for hash {torrent_hash}: {e}")
            raise

    def add_torrent(self, torrent_file_content: bytes, save_path: str, is_paused: bool, category: str, use_auto_management: bool) -> bool:
        """Adds a new torrent to the client."""
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            result = self.client.torrents_add(
                torrent_files=torrent_file_content,
                save_path=save_path,
                is_paused=is_paused,
                category=category,
                use_auto_torrent_management=use_auto_management
            )
            return result == 'Ok.'
        except Exception as e:
            logging.error(f"Failed to add torrent: {e}")
            return False

    def recheck_torrent(self, torrent_hash: str) -> bool:
        """Forces a recheck of the torrent data."""
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            self.client.torrents_recheck(torrent_hashes=torrent_hash)
            return True
        except qbittorrentapi.exceptions.NotFound404Error:
            logging.error(f"Failed to start recheck: Torrent {torrent_hash} not found.")
            return False
        except Exception as e:
            logging.error(f"Failed to recheck torrent {torrent_hash}: {e}")
            return False

    def resume_torrent(self, torrent_hash: str) -> bool:
        """Resumes a paused torrent."""
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            self.client.torrents_resume(torrent_hashes=torrent_hash)
            return True
        except Exception as e:
            logging.error(f"Failed to resume torrent {torrent_hash}: {e}")
            return False

    def pause_torrent(self, torrent_hash: str) -> bool:
        """Pauses a torrent."""
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            self.client.torrents_pause(torrent_hashes=torrent_hash)
            return True
        except Exception as e:
            logging.error(f"Failed to pause torrent {torrent_hash}: {e}")
            return False

    def delete_torrent(self, torrent_hash: str, delete_files: bool) -> bool:
        """Deletes a torrent and optionally its data."""
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            self.client.torrents_delete(torrent_hashes=torrent_hash, delete_files=delete_files)
            return True
        except qbittorrentapi.exceptions.NotFound404Error:
            logging.warning(f"Source torrent {torrent_hash} already deleted or not found.")
            return True # It's already gone, so that's a success in this context
        except Exception as e:
            logging.warning(f"Failed to delete source torrent {torrent_hash}: {e}")
            return False

    def get_torrent_info(self, torrent_hash: str) -> Optional[Any]:
        """Gets information about a specific torrent."""
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            torrents = self.client.torrents_info(torrent_hashes=torrent_hash)
            return torrents[0] if torrents else None
        except Exception as e:
            logging.error(f"Could not get info for torrent {torrent_hash}: {e}")
            return None


    def wait_for_recheck_completion(
        self,
        torrent_hash: str,
        ui: "BaseUIManager",
        no_progress_timeout: int,
        dry_run: bool = False
    ) -> str:
        """
        Monitors a torrent's recheck, returning a status string.

        Args:
            torrent_hash: The hash of the torrent to monitor.
            ui: The user interface manager for watchdog petting.
            no_progress_timeout: Seconds to wait before timing out if no progress is made.
            dry_run: If True, simulates a successful recheck.

        Returns:
            "SUCCESS": If the recheck completes to 100%.
            "FAILED_STATE": If the torrent enters an error state or disappears.
            "FAILED_TIMEOUT": If no progress is made for the timeout duration.
        """
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        if dry_run:
            logging.info(f"[DRY RUN] Would wait for recheck on {torrent_hash[:10]}. Assuming success.")
            return "SUCCESS"

        last_progress_time = time.time()
        last_progress = -1

        logging.info(f"Waiting for recheck to complete for torrent {torrent_hash[:10]}...")

        while True:
            ui.pet_watchdog()
            try:
                torrent_info = self.client.torrents_info(torrent_hashes=torrent_hash)
                if not torrent_info:
                    logging.error(f"Torrent {torrent_hash[:10]} disappeared while waiting for recheck.")
                    return "FAILED_STATE"

                torrent = torrent_info[0]
                state = torrent.state
                current_progress = torrent.progress

                # Success Condition (Directive 2): Check for exactly 100%
                if current_progress >= 1.0:
                    logging.info(f"Recheck completed for torrent {torrent_hash[:10]} at 100%.")
                    return "SUCCESS"

                # Failure Condition (Directive 1): Check for error or stopped states
                if state in ['error', 'missingFiles', 'stopped', 'pausedUP', 'pausedDL']:
                    logging.error(f"Recheck FAILED for torrent {torrent_hash[:10]}. State is '{state}'.")
                    return "FAILED_STATE"

                # Timeout Condition (Directive 1)
                if current_progress > last_progress:
                    last_progress_time = time.time()
                    last_progress = current_progress

                if time.time() - last_progress_time > no_progress_timeout:
                    logging.error(f"Recheck FAILED for {torrent_hash[:10]}: No progress for {no_progress_timeout} seconds.")
                    return "FAILED_TIMEOUT"

                logging.debug(f"Recheck progress: {current_progress*100:.2f}% (State: {state})")
                time.sleep(10)

            except Exception as e:
                logging.error(f"Error while waiting for recheck on {torrent_hash[:10]}: {e}", exc_info=True)
                return "FAILED_STATE"

    def get_incomplete_files(self, torrent_hash: str) -> List[str]:
        """
        Gets a list of incomplete files for a given torrent.

        Args:
            torrent_hash: The hash of the torrent to check.

        Returns:
            A list of file names (paths relative to torrent root) that are not 100% complete.
        """
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            files = self.client.torrents_files(torrent_hash=torrent_hash)
            incomplete_files = [f.name for f in files if f.progress < 1]
            if incomplete_files:
                logging.warning(f"Found {len(incomplete_files)} incomplete files for torrent {torrent_hash[:10]}...")
            return incomplete_files
        except Exception as e:
            logging.error(f"Could not get file list for torrent {torrent_hash[:10]}: {e}")
            return []

    def get_all_torrents(self, **kwargs) -> List[Any]:
        """Gets a list of all torrents in the client."""
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            return self.client.torrents_info(**kwargs)
        except Exception as e:
            logging.error(f"Could not get all torrents: {e}")
            return []

    def set_torrent_category(self, torrent_hash: str, category: str) -> bool:
        """Sets the category for a given torrent."""
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            self.client.torrents_set_category(torrent_hashes=torrent_hash, category=category)
            return True
        except Exception as e:
            logging.error(f"Failed to set category for torrent {torrent_hash}: {e}")
            return False

    def get_torrent_trackers(self, torrent_hash: str) -> List[Any]:
        """Gets the trackers for a given torrent."""
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            return self.client.torrents_trackers(torrent_hash=torrent_hash)
        except Exception as e:
            logging.error(f"Failed to get trackers for torrent {torrent_hash}: {e}")
            return []

    def get_torrent_categories(self) -> List[Any]:
        """Gets the categories of the client."""
        if not self.client:
            raise ConnectionError("Client not connected. Call connect() first.")
        try:
            return self.client.torrent_categories.categories
        except Exception as e:
            logging.error(f"Failed to get categories: {e}")
            return []
