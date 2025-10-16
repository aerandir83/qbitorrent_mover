import abc
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class Torrent:
    """A dataclass to hold standardized torrent information."""
    hash: str
    name: str
    category: str
    content_path: str
    size: int
    added_on: int
    progress: float
    state: str
    # This holds the original torrent object from the client library for any specific needs
    raw_torrent: Any


class TorrentClient(abc.ABC):
    """
    An abstract base class for a torrent client.
    """

    def __init__(self, config: Dict[str, str]):
        """Initializes the client with its specific configuration section."""
        self.config = config
        self.client = None

    @abc.abstractmethod
    def connect(self) -> None:
        """Connects to the torrent client. Raises an exception on failure."""
        pass

    @abc.abstractmethod
    def get_torrents_to_move(self, category: str, size_threshold_gb: Optional[float] = None) -> List[Torrent]:
        """
        Retrieves a list of torrents eligible for moving based on category and optional size threshold.
        """
        pass

    @abc.abstractmethod
    def add_torrent(self, torrent_content: Optional[bytes], save_path: str, is_paused: bool, category: str, use_auto_tm: bool, torrent_url: Optional[str] = None) -> None:
        """Adds a torrent to the client, either via file content or magnet URI."""
        pass

    @abc.abstractmethod
    def remove_torrent(self, torrent_hash: str, delete_files: bool) -> None:
        """Removes a torrent from the client."""
        pass

    @abc.abstractmethod
    def recheck(self, torrent_hash: str) -> None:
        """Triggers a recheck for a torrent."""
        pass

    @abc.abstractmethod
    def resume(self, torrent_hash: str) -> None:
        """Resumes a torrent."""
        pass

    @abc.abstractmethod
    def pause(self, torrent_hash: str) -> None:
        """Pauses a torrent."""
        pass

    @abc.abstractmethod
    def get_torrent_info(self, torrent_hash: str) -> Optional[Torrent]:
        """Gets detailed information for a single torrent."""
        pass

    @abc.abstractmethod
    def export_torrent(self, torrent_hash: str) -> Optional[bytes]:
        """Exports the .torrent file content. Returns None if not supported."""
        pass

    @abc.abstractmethod
    def get_torrent_url(self, torrent_hash: str) -> Optional[str]:
        """Gets the magnet URI for a torrent. Returns None if not available."""
        pass

    @abc.abstractmethod
    def get_all_categories(self) -> Dict[str, Any]:
        """Gets all available categories from the client."""
        pass

    @abc.abstractmethod
    def set_category(self, torrent_hash: str, category: str) -> None:
        """Sets the category for a torrent."""
        pass

    @abc.abstractmethod
    def get_trackers(self, torrent_hash: str) -> List[Dict[str, Any]]:
        """Gets the list of trackers for a torrent."""
        pass