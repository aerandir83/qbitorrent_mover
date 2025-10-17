import logging
import base64
import os
import time
from typing import List, Dict, Any, Optional

from deluge_web_client import DelugeWebClient

from .base import TorrentClient, Torrent
from ..utils import retry

class DelugeClient(TorrentClient):
    """
    A Deluge client implementation that connects to the Deluge Web UI.
    """

    @retry(tries=2, delay=5)
    def connect(self) -> None:
        """Connects to the Deluge Web UI."""
        base_url = self.config.get('host').value
        password = self.config.get('password').value

        logging.info(f"Connecting to Deluge Web UI at {base_url}...")
        self.client = DelugeWebClient(base_url=base_url, password=password)
        self.client.connect()
        logging.info("Successfully connected to Deluge Web UI.")

    def _to_torrent_dataclass(self, deluge_torrent: Dict[str, Any]) -> Torrent:
        """Converts a Deluge torrent dictionary to the standardized Torrent dataclass."""
        save_path = deluge_torrent.get('save_path', '')
        # The Web UI API returns a combined path in 'save_path', but sometimes it's just the directory.
        # The most reliable content path is the save_path joined with the torrent name,
        # as rsync can handle both file and directory sources correctly.
        content_path = os.path.join(save_path, deluge_torrent['name'])

        return Torrent(
            hash=deluge_torrent['hash'],
            name=deluge_torrent['name'],
            category=deluge_torrent.get('label', ''),
            content_path=content_path,
            size=deluge_torrent['total_size'],
            added_on=int(deluge_torrent['time_added']),
            progress=deluge_torrent['progress'] / 100.0,
            state=deluge_torrent['state'],
            raw_torrent=deluge_torrent,
        )

    def get_torrents_to_move(self, category: str, size_threshold_gb: Optional[float] = None) -> List[Torrent]:
        """Retrieves a list of torrents to be moved from Deluge."""
        all_torrents = self.client.get_torrents_status()

        torrents_in_category = [t for t in all_torrents.values() if t.get('label') == category]

        if size_threshold_gb is None:
            completed_torrents = [t for t in torrents_in_category if t['progress'] == 100]
            logging.info(f"Found {len(completed_torrents)} completed torrent(s) in category '{category}' to move.")
            return [self._to_torrent_dataclass(t) for t in completed_torrents]

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
        """Adds a torrent to Deluge via the Web UI."""
        hashes_before = set(self.client.get_torrents_status().keys())

        if torrent_url:
            self.client.add_torrent_magnet(magnet_uri=torrent_url, download_location=save_path)
        elif torrent_content:
            self.client.add_torrent_file(file_content=torrent_content, download_location=save_path)
        else:
            raise ValueError("Either torrent_content or torrent_url must be provided.")

        time.sleep(2)

        hashes_after = set(self.client.get_torrents_status().keys())
        new_hashes = hashes_after - hashes_before

        if not new_hashes:
            logging.error("Could not identify hash of newly added torrent.")
            return

        torrent_hash = new_hashes.pop()
        logging.info(f"Identified new torrent hash: {torrent_hash}")

        options = {}
        if is_paused:
            self.client.pause_torrent(torrent_ids=[torrent_hash])
        if category:
            options['label'] = category
        if options:
            self.client.set_torrent_options(torrent_ids=[torrent_hash], options=options)

    def remove_torrent(self, torrent_hash: str, delete_files: bool) -> None:
        """Removes a torrent from Deluge."""
        self.client.remove_torrent(torrent_id=torrent_hash, remove_data=delete_files)

    def recheck(self, torrent_hash: str) -> None:
        """Triggers a recheck for a torrent."""
        self.client.force_recheck(torrent_ids=[torrent_hash])

    def resume(self, torrent_hash: str) -> None:
        """Resumes a torrent."""
        self.client.resume_torrent(torrent_ids=[torrent_hash])

    def pause(self, torrent_hash: str) -> None:
        """Pauses a torrent."""
        self.client.pause_torrent(torrent_ids=[torrent_hash])

    def get_torrent_info(self, torrent_hash: str) -> Optional[Torrent]:
        """Gets detailed information for a single torrent."""
        all_torrents = self.client.get_torrents_status()
        torrent_data = all_torrents.get(torrent_hash)
        if torrent_data:
            return self._to_torrent_dataclass(torrent_data)
        return None

    def export_torrent(self, torrent_hash: str) -> Optional[bytes]:
        """Deluge Web UI does not support exporting .torrent files directly."""
        logging.warning("Deluge client does not support exporting .torrent files. Will rely on magnet URI.")
        return None

    def get_torrent_url(self, torrent_hash: str) -> Optional[str]:
        """Gets the magnet URI for a torrent."""
        all_torrents = self.client.get_torrents_status()
        torrent = all_torrents.get(torrent_hash)
        if torrent:
            trackers = [t['url'] for t in torrent.get('trackers', [])]
            magnet_uri = f"magnet:?xt=urn:btih:{torrent['hash']}&dn={torrent['name']}"
            for tracker in trackers:
                magnet_uri += f"&tr={tracker}"
            return magnet_uri
        return None

    def get_all_categories(self) -> Dict[str, Any]:
        """Gets all available labels (categories) from Deluge."""
        all_torrents = self.client.get_torrents_status()
        labels = set(t.get('label') for t in all_torrents.values() if t.get('label'))
        return {label: {} for label in labels}

    def set_category(self, torrent_hash: str, category: str) -> None:
        """Sets the label (category) for a torrent."""
        self.client.set_torrent_options(torrent_ids=[torrent_hash], options={'label': category})

    def get_trackers(self, torrent_hash: str) -> List[Dict[str, Any]]:
        """Gets the list of trackers for a torrent."""
        all_torrents = self.client.get_torrents_status()
        torrent_info = all_torrents.get(torrent_hash)
        if torrent_info and 'trackers' in torrent_info:
            return torrent_info['trackers']
        return []