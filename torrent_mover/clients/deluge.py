import logging
import base64
from typing import List, Dict, Any, Optional

from deluge_client import DelugeRPCClient

from .base import TorrentClient, Torrent
from ..utils import retry

class DelugeClient(TorrentClient):
    """
    A Deluge client implementation.
    """

    @retry(tries=2, delay=5)
    def connect(self) -> None:
        """Connects to the Deluge daemon."""
        host = self.config.get('host')
        port = int(self.config.get('port'))
        username = self.config.get('username')
        password = self.config.get('password')

        logging.info(f"Connecting to Deluge at {host}:{port}...")
        self.client = DelugeRPCClient(host, port, username, password, decode_utf8=True)
        self.client.connect()
        logging.info(f"Successfully connected to Deluge.")

    def _to_torrent_dataclass(self, deluge_torrent: Dict[str, Any]) -> Torrent:
        """Converts a Deluge torrent dictionary to the standardized Torrent dataclass."""
        # Deluge's 'state' is more detailed, we can map it to broader categories if needed
        # For now, using it directly.
        return Torrent(
            hash=deluge_torrent['hash'],
            name=deluge_torrent['name'],
            # Deluge has a 'label' plugin for categories. If not installed, this might be empty.
            category=deluge_torrent.get('label', ''),
            content_path=deluge_torrent['save_path'], # Note: This is the directory, not the content path itself
            size=deluge_torrent['total_size'],
            added_on=int(deluge_torrent['time_added']),
            progress=deluge_torrent['progress'],
            state=deluge_torrent['state'],
            raw_torrent=deluge_torrent,
        )

    def get_torrents_to_move(self, category: str, size_threshold_gb: Optional[float] = None) -> List[Torrent]:
        """Retrieves a list of torrents to be moved from Deluge."""
        # Deluge uses the 'label' plugin for categories.
        # We fetch all torrents and filter by the label.
        all_torrents = self.client.call('core.get_torrents_status', {}, ['name', 'hash', 'label', 'save_path', 'total_size', 'time_added', 'progress', 'state'])

        # Filter by category (label)
        torrents_in_category = [t for t in all_torrents.values() if t.get('label') == category]

        if size_threshold_gb is None:
            completed_torrents = [t for t in torrents_in_category if t['progress'] == 100]
            logging.info(f"Found {len(completed_torrents)} completed torrent(s) in category '{category}' to move.")
            return [self._to_torrent_dataclass(t) for t in completed_torrents]

        # --- Threshold logic ---
        logging.info(f"Size threshold of {size_threshold_gb} GB is active for category '{category}'.")
        current_total_size = sum(t['total_size'] for t in torrents_in_category)
        threshold_bytes = size_threshold_gb * (1024**3)

        logging.info(f"Current category size: {current_total_size / (1024**3):.2f} GB. Target size: {size_threshold_gb:.2f} GB.")

        if current_total_size <= threshold_bytes:
            logging.info("Category size is already below the threshold. No torrents to move.")
            return []

        size_to_move = current_total_size - threshold_bytes
        logging.info(f"Need to move at least {size_to_move / (1024**3):.2f} GB of torrents.")

        completed_torrents = [t for t in torrents_in_category if t['progress'] == 100]
        eligible_torrents = sorted(completed_torrents, key=lambda t: t['time_added'])

        torrents_to_move = []
        size_of_selected_torrents = 0
        for torrent in eligible_torrents:
            if size_of_selected_torrents >= size_to_move:
                break
            torrents_to_move.append(torrent)
            size_of_selected_torrents += torrent['total_size']

        if not torrents_to_move:
            logging.warning("No completed torrents are available to move to meet the threshold.")
            return []

        logging.info(f"Selected {len(torrents_to_move)} torrent(s) to meet the threshold (Total size: {size_of_selected_torrents / (1024**3):.2f} GB).")
        return [self._to_torrent_dataclass(t) for t in torrents_to_move]

    def add_torrent(self, torrent_content: Optional[bytes], save_path: str, is_paused: bool, category: str, use_auto_tm: bool, torrent_url: Optional[str] = None) -> None:
        """Adds a torrent to Deluge."""
        options = {
            'download_location': save_path,
            'add_paused': is_paused
        }
        if category:
            # Requires the 'label' plugin to be enabled in Deluge
            options['label'] = category

        if torrent_url:
            self.client.call('core.add_torrent_magnet', torrent_url, options)
        elif torrent_content:
            b64_content = base64.b64encode(torrent_content).decode('utf-8')
            # The first argument to add_torrent_file is the filename, which is not available here.
            # We can pass an empty string, as it's primarily for display.
            self.client.call('core.add_torrent_file', '', b64_content, options)
        else:
            raise ValueError("Either torrent_content or torrent_url must be provided.")

    def remove_torrent(self, torrent_hash: str, delete_files: bool) -> None:
        """Removes a torrent from Deluge."""
        self.client.call('core.remove_torrent', torrent_hash, delete_files)

    def recheck(self, torrent_hash: str) -> None:
        """Triggers a recheck for a torrent."""
        self.client.call('core.force_recheck', [torrent_hash])

    def resume(self, torrent_hash: str) -> None:
        """Resumes a torrent."""
        self.client.call('core.resume_torrent', [torrent_hash])

    def pause(self, torrent_hash: str) -> None:
        """Pauses a torrent."""
        self.client.call('core.pause_torrent', [torrent_hash])

    def get_torrent_info(self, torrent_hash: str) -> Optional[Torrent]:
        """Gets detailed information for a single torrent."""
        torrent_data = self.client.call('core.get_torrents_status', {'hash': torrent_hash}, [])
        if torrent_data:
            return self._to_torrent_dataclass(list(torrent_data.values())[0])
        return None

    def export_torrent(self, torrent_hash: str) -> Optional[bytes]:
        """Deluge RPC does not support exporting .torrent files directly."""
        logging.warning("Deluge client does not support exporting .torrent files. Will rely on magnet URI.")
        return None

    def get_torrent_url(self, torrent_hash: str) -> Optional[str]:
        """Gets the magnet URI for a torrent."""
        # This requires the webui to be running to construct the magnet URI.
        # A bit of a hack, but it's the most reliable way.
        try:
            torrent_info = self.client.call('web.get_torrent_status', torrent_hash, ['magnet_uri'])
            return torrent_info.get('magnet_uri')
        except Exception as e:
            logging.error(f"Could not get magnet URI for {torrent_hash} from Deluge Web API: {e}")
            return None

    def get_all_categories(self) -> Dict[str, Any]:
        """Gets all available labels (categories) from Deluge."""
        # Requires the 'label' plugin
        try:
            labels = self.client.call('label.get_labels')
            return {label: {} for label in labels} # Mimic qBittorrent's category structure
        except Exception:
            logging.warning("Could not fetch labels from Deluge. Is the 'label' plugin enabled?")
            return {}

    def set_category(self, torrent_hash: str, category: str) -> None:
        """Sets the label (category) for a torrent."""
        try:
            self.client.call('label.set_torrent', torrent_hash, category)
        except Exception:
            logging.warning(f"Could not set label '{category}' for torrent {torrent_hash}. Is the 'label' plugin enabled?")

    def get_trackers(self, torrent_hash: str) -> List[Dict[str, Any]]:
        """Gets the list of trackers for a torrent."""
        torrent_info = self.client.call('core.get_torrent_status', torrent_hash, ['trackers'])
        if torrent_info and 'trackers' in torrent_info:
            # Deluge's tracker format is already a list of dicts with a 'url' key
            return torrent_info['trackers']
        return []