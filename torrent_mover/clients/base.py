from abc import ABC, abstractmethod
from collections import namedtuple

SimpleTorrent = namedtuple(
    "SimpleTorrent",
    [
        "hash",
        "name",
        "category",
        "size",
        "state",
        "progress",
        "added_on",
        "content_path",
    ],
)

class TorrentClient(ABC):
    """
    An abstract base class for a torrent client.
    """

    def __init__(self, config_section):
        self.config = config_section

    @abstractmethod
    def connect(self):
        """Connects to the torrent client."""
        pass

    @abstractmethod
    def get_app_version(self):
        """Returns the version of the torrent client application."""
        pass

    @abstractmethod
    def get_torrents(self, **kwargs):
        """
        Returns a list of torrents.
        The torrents should be returned as a list of SimpleTorrent objects.
        """
        pass

    @abstractmethod
    def export_torrent(self, torrent_hash):
        """Returns the content of a .torrent file for the given hash."""
        pass

    @abstractmethod
    def get_magnet_uri(self, torrent_hash):
        """Returns the magnet URI for the given torrent hash."""
        pass

    @abstractmethod
    def add_torrent(self, torrent_data, **kwargs):
        """Adds a new torrent to the client. torrent_data can be file content (bytes) or a magnet link (str)."""
        pass

    @abstractmethod
    def recheck_torrent(self, torrent_hashes):
        """Rechecks the specified torrents."""
        pass

    @abstractmethod
    def resume_torrent(self, torrent_hashes):
        """Resumes the specified torrents."""
        pass

    @abstractmethod
    def pause_torrent(self, torrent_hashes):
        """Pauses the specified torrents."""
        pass

    @abstractmethod
    def delete_torrent(self, torrent_hashes, delete_files):
        """Deletes the specified torrents."""
        pass

    @abstractmethod
    def set_category(self, torrent_hashes, category):
        """Sets the category for the specified torrents."""
        pass

    @abstractmethod
    def get_trackers(self, torrent_hash):
        """Returns the trackers for a specific torrent."""
        pass

    @abstractmethod
    def get_categories(self):
        """Returns a list of available categories."""
        pass
