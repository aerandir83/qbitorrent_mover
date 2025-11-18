import configparser
from typing import List, Dict, Optional

class MockTorrent:
    """
    A mock for the qbittorrentapi.TorrentDictionary object.
    We will add attributes here as needed for tests.
    """
    def __init__(self, name: str, hash_: str, content_path: str, save_path: str, category: str = "", progress: float = 1.0, state: str = "uploading"):
        self.name = name
        self.hash_ = hash_
        self.content_path = content_path
        self.save_path = save_path
        self.category = category
        self.progress = progress
        self.state = state
        # Add other attributes as needed
        self.added_on = 0
        self.size = 0

    def __getitem__(self, key):
        """Allow dictionary-style access for compatibility."""
        return getattr(self, key)

class MockQBittorrentClient:
    """
    A mock for the qbittorrentapi.Client.
    We will add mock methods here as needed for tests.
    """
    def __init__(self, config_section: configparser.SectionProxy, client_name: str):
        self.client_name = client_name
        self.torrents: List[MockTorrent] = []
        self.categories = {}
        self.app = self
        self.version = "mock-4.4.0"

    def auth_log_in(self):
        # Mock login is always successful
        pass

    def torrents_info(self, torrent_hashes: Optional[List[str]] = None, **kwargs) -> List[MockTorrent]:
        if torrent_hashes:
            return [t for t in self.torrents if t.hash_ in torrent_hashes]
        return self.torrents

    def torrents_recheck(self, torrent_hashes: List[str]):
        # Mock recheck
        pass

    def torrents_delete(self, torrent_hashes: List[str], delete_files: bool = False):
        # Mock delete
        pass

    # Add other mock methods like torrents_add, torrents_set_category, etc.
    # as we need them in future tests.