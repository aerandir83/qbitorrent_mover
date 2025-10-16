import logging
from typing import List, Dict, Any, Optional

import qbittorrentapi
from qbittorrentapi.exceptions import APIError

from .base import TorrentClient, Torrent
from ..utils import retry

class QBittorrentClient(TorrentClient):
    """
    A qBittorrent client implementation.
    """

    @retry(tries=2, delay=5)
    def connect(self) -> None:
        """Connects to the qBittorrent client."""
        host = self.config.get('host').value
        port = self.config.get('port').value
        username = self.config.get('username').value
        password = self.config.get('password').value
        verify_cert_opt = self.config.get('verify_cert')
        verify_cert = verify_cert_opt.value.lower() == 'true' if verify_cert_opt and verify_cert_opt.value else True

        logging.info(f"Connecting to qBittorrent at {host}...")
        self.client = qbittorrentapi.Client(
            host=host,
            port=port,
            username=username,
            password=password,
            VERIFY_WEBUI_CERTIFICATE=verify_cert,
            REQUESTS_ARGS={'timeout': 20}
        )
        self.client.auth_log_in()
        logging.info(f"Successfully connected to qBittorrent. Version: {self.client.app.version}")

    def _to_torrent_dataclass(self, qbit_torrent: qbittorrentapi.TorrentDictionary) -> Torrent:
        """Converts a qBittorrent torrent object to the standardized Torrent dataclass."""
        return Torrent(
            hash=qbit_torrent.hash,
            name=qbit_torrent.name,
            category=qbit_torrent.category,
            content_path=qbit_torrent.content_path,
            size=qbit_torrent.size,
            added_on=qbit_torrent.added_on,
            progress=qbit_torrent.progress,
            state=qbit_torrent.state,
            raw_torrent=qbit_torrent,
        )

    def get_torrents_to_move(self, category: str, size_threshold_gb: Optional[float] = None) -> List[Torrent]:
        """Retrieves a list of torrents to be moved."""
        if size_threshold_gb is None:
            torrents = self.client.torrents_info(category=category, status_filter='completed')
            logging.info(f"Found {len(torrents)} completed torrent(s) in category '{category}' to move.")
            return [self._to_torrent_dataclass(t) for t in torrents]

        logging.info(f"Size threshold of {size_threshold_gb} GB is active for category '{category}'.")
        all_torrents_in_category = self.client.torrents_info(category=category)

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
        return [self._to_torrent_dataclass(t) for t in torrents_to_move]

    def add_torrent(self, torrent_content: Optional[bytes], save_path: str, is_paused: bool, category: str, use_auto_tm: bool, torrent_url: Optional[str] = None) -> None:
        """Adds a torrent to the client."""
        self.client.torrents_add(
            torrent_files=torrent_content,
            urls=torrent_url,
            save_path=save_path,
            is_paused=is_paused,
            category=category,
            use_auto_torrent_management=use_auto_tm
        )

    def remove_torrent(self, torrent_hash: str, delete_files: bool) -> None:
        """Removes a torrent from the client."""
        self.client.torrents_delete(torrent_hashes=torrent_hash, delete_files=delete_files)

    def recheck(self, torrent_hash: str) -> None:
        """Triggers a recheck for a torrent."""
        self.client.torrents_recheck(torrent_hashes=torrent_hash)

    def resume(self, torrent_hash: str) -> None:
        """Resumes a torrent."""
        self.client.torrents_resume(torrent_hashes=torrent_hash)

    def pause(self, torrent_hash: str) -> None:
        """Pauses a torrent."""
        self.client.torrents_pause(torrent_hashes=torrent_hash)

    def get_torrent_info(self, torrent_hash: str) -> Optional[Torrent]:
        """Gets detailed information for a single torrent."""
        try:
            torrent_info = self.client.torrents_info(torrent_hashes=torrent_hash)
            if torrent_info:
                return self._to_torrent_dataclass(torrent_info[0])
        except APIError as e:
            logging.warning(f"API error getting torrent info for {torrent_hash}: {e}")
        return None

    def export_torrent(self, torrent_hash: str) -> Optional[bytes]:
        """Exports the .torrent file content."""
        try:
            return self.client.torrents_export(torrent_hash=torrent_hash)
        except APIError as e:
            logging.warning(f"Could not export .torrent file for hash {torrent_hash}: {e}")
            return None

    def get_torrent_url(self, torrent_hash: str) -> Optional[str]:
        """qBittorrent does not have a direct way to get a magnet URI for an existing torrent."""
        logging.debug("qBittorrent client does not support fetching magnet URI for existing torrents.")
        return None

    def get_all_categories(self) -> Dict[str, Any]:
        """Gets all available categories from the client."""
        return self.client.torrent_categories.categories

    def set_category(self, torrent_hash: str, category: str) -> None:
        """Sets the category for a torrent."""
        self.client.torrents_set_category(torrent_hashes=torrent_hash, category=category)

    def get_trackers(self, torrent_hash: str) -> List[Dict[str, Any]]:
        """Gets the list of trackers for a torrent."""
        return self.client.torrents_trackers(torrent_hash=torrent_hash)