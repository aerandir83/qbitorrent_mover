from abc import ABC, abstractmethod
from typing import List, Optional, Any

class TorrentClient(ABC):
    """Abstract Base Class for Torrent Clients."""

    @abstractmethod
    def connect(self) -> bool:
        pass

    @abstractmethod
    def get_eligible_torrents(self, category: str, size_threshold_gb: Optional[float] = None) -> List[Any]:
        pass

    @abstractmethod
    def get_torrent_info(self, torrent_hash: str) -> Optional[Any]:
        pass

    @abstractmethod
    def pause_torrent(self, torrent_hash: str) -> None:
        pass

    @abstractmethod
    def resume_torrent(self, torrent_hash: str) -> None:
        pass

    @abstractmethod
    def delete_torrent(self, torrent_hash: str, delete_files: bool = False) -> None:
        pass

    @abstractmethod
    def recheck_torrent(self, torrent_hash: str) -> None:
        pass

    @abstractmethod
    def set_category(self, torrent_hash: str, category: str) -> None:
        pass

    @abstractmethod
    def add_torrent(self, torrent_files: bytes, save_path: str, category: str, is_paused: bool = False) -> None:
        pass

    @abstractmethod
    def export_torrent(self, torrent_hash: str) -> bytes:
        pass

    @abstractmethod
    def get_incomplete_files(self, torrent_hash: str) -> List[str]:
        pass

    @abstractmethod
    def wait_for_recheck(self, torrent_hash: str, ui_manager: Any, stuck_timeout: int, stopped_timeout: int, grace_period: int = 0, dry_run: bool = False) -> str:
        pass
