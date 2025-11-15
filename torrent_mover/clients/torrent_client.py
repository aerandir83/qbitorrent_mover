from abc import ABC, abstractmethod
from typing import Any, List, Optional

class TorrentClientInterface(ABC):
    """
    An abstract base class that defines the interface for a torrent client.
    """

    @abstractmethod
    def connect(self):
        """Connects to the torrent client."""
        pass

    @abstractmethod
    def get_eligible_torrents(self, category: str, size_threshold_gb: Optional[float]) -> List[Any]:
        """Retrieves a list of torrents eligible for transfer."""
        pass

    @abstractmethod
    def export_torrent_file(self, torrent_hash: str) -> bytes:
        """Exports the .torrent file for a given torrent."""
        pass

    @abstractmethod
    def add_torrent(self, torrent_file_content: bytes, save_path: str, is_paused: bool, category: str, use_auto_management: bool) -> bool:
        """Adds a new torrent to the client."""
        pass

    @abstractmethod
    def recheck_torrent(self, torrent_hash: str) -> bool:
        """Forces a recheck of the torrent data."""
        pass

    @abstractmethod
    def resume_torrent(self, torrent_hash: str) -> bool:
        """Resumes a paused torrent."""
        pass

    @abstractmethod
    def pause_torrent(self, torrent_hash: str) -> bool:
        """Pauses a torrent."""
        pass

    @abstractmethod
    def delete_torrent(self, torrent_hash: str, delete_files: bool) -> bool:
        """Deletes a torrent and optionally its data."""
        pass

    @abstractmethod
    def get_torrent_info(self, torrent_hash: str) -> Optional[Any]:
        """Gets information about a specific torrent."""
        pass

    @abstractmethod
    def wait_for_recheck_completion(self, torrent_hash: str, ui: "BaseUIManager", no_progress_timeout: int, dry_run: bool) -> str:
        """Monitors a torrent's recheck, returning a status string."""
        pass

    @abstractmethod
    def get_incomplete_files(self, torrent_hash: str) -> List[str]:
        """Gets a list of incomplete files for a given torrent."""
        pass

    @abstractmethod
    def get_all_torrents(self, **kwargs) -> List[Any]:
        """Gets a list of all torrents in the client."""
        pass

    @abstractmethod
    def set_torrent_category(self, torrent_hash: str, category: str) -> bool:
        """Sets the category for a given torrent."""
        pass

    @abstractmethod
    def get_torrent_trackers(self, torrent_hash: str) -> List[Any]:
        """Gets the trackers for a given torrent."""
        pass

    @abstractmethod
    def get_torrent_categories(self) -> List[Any]:
        """Gets the categories of the client."""
        pass
