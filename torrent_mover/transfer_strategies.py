from __future__ import annotations
import abc
import dataclasses
import logging
import os
import shlex

from typing import cast, Dict, List, TYPE_CHECKING, Tuple


if TYPE_CHECKING:
    from .ssh_manager import SSHConnectionPool
    from .torrent_mover import Torrent

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class TransferFile:
    """Normalized file representation."""
    source_path: str
    dest_path: str
    size: int
    torrent_hash: str


class TransferStrategy(abc.ABC):
    """Abstract base class for different transfer strategies."""

    def __init__(self, config: Dict, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        self.config = config
        self.ssh_connection_pools = ssh_connection_pools

    @abc.abstractmethod
    def prepare_files(self, torrent: "Torrent", dest_path: str) -> List[TransferFile]:
        """Prepare a list of files to be transferred for a given torrent."""
        pass

    @abc.abstractmethod
    def supports_parallel(self) -> bool:
        """Returns True if the strategy supports parallel file transfers."""
        pass


class SFTPStrategy(TransferStrategy):
    """Strategy for handling file transfers using SFTP."""

    def __init__(self, config: Dict, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.pool = self.ssh_connection_pools[source_server_section]

    def supports_parallel(self) -> bool:
        return True

    def prepare_files(self, torrent: "Torrent", dest_path: str) -> List[TransferFile]:
        """
        Recursively lists all files from the source SFTP server and prepares them for transfer.
        """
        from .ssh_manager import _get_all_files_recursive
        source_path = torrent.content_path.rstrip('/\\')
        file_list: List[Tuple[str, str, int]] = []
        with self.pool.get_connection() as (sftp, ssh):
            _get_all_files_recursive(sftp, source_path, dest_path, file_list)

        return [
            TransferFile(
                source_path=src,
                dest_path=dest,
                size=size,
                torrent_hash=torrent.hash
            ) for src, dest, size in file_list
        ]

class RsyncStrategy(TransferStrategy):
    """Strategy for handling file transfers using rsync."""

    def __init__(self, config: Dict, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.pool = self.ssh_connection_pools[source_server_section]

    def supports_parallel(self) -> bool:
        return False

    def prepare_files(self, torrent: "Torrent", dest_path: str) -> List[TransferFile]:
        """
        Prepares a single TransferFile representing the entire torrent for rsync.
        """
        from .ssh_manager import batch_get_remote_sizes
        source_path = torrent.content_path.rstrip('/\\')
        with self.pool.get_connection() as (sftp, ssh):
            # batch_get_remote_sizes returns a dict, we need the value
            sizes = batch_get_remote_sizes(ssh, [source_path])
            total_size = sizes.get(source_path, 0)

        return [
            TransferFile(
                source_path=source_path,
                dest_path=dest_path,
                size=total_size,
                torrent_hash=torrent.hash
            )
        ]

class SFTPUploadStrategy(TransferStrategy):
    """Strategy for handling SFTP-to-SFTP transfers."""
    def __init__(self, config: Dict, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.pool = self.ssh_connection_pools[source_server_section]

    def supports_parallel(self) -> bool:
        return True

    def prepare_files(self, torrent: "Torrent", dest_path: str) -> List[TransferFile]:
        from .ssh_manager import _get_all_files_recursive
        source_path = torrent.content_path.rstrip('/\\')
        file_list: List[Tuple[str, str, int]] = []
        with self.pool.get_connection() as (sftp, ssh):
            _get_all_files_recursive(sftp, source_path, dest_path, file_list)

        return [
            TransferFile(
                source_path=src,
                dest_path=dest,
                size=size,
                torrent_hash=torrent.hash
            ) for src, dest, size in file_list
        ]


class RsyncUploadStrategy(TransferStrategy):
    """Strategy for handling rsync-based server-to-server transfers."""
    def __init__(self, config: Dict, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.pool = self.ssh_connection_pools[source_server_section]

    def supports_parallel(self) -> bool:
        return False

    def prepare_files(self, torrent: "Torrent", dest_path: str) -> List[TransferFile]:
        from .ssh_manager import batch_get_remote_sizes
        source_path = torrent.content_path.rstrip('/\\')
        with self.pool.get_connection() as (sftp, ssh):
            sizes = batch_get_remote_sizes(ssh, [source_path])
            total_size = sizes.get(source_path, 0)

        return [
            TransferFile(
                source_path=source_path,
                dest_path=dest_path,
                size=total_size,
                torrent_hash=torrent.hash
            )
        ]


def get_transfer_strategy(mode: str, config: Dict, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> TransferStrategy:
    """Factory function to get the appropriate transfer strategy."""
    strategies = {
        "sftp": SFTPStrategy,
        "rsync": RsyncStrategy,
        "sftp_upload": SFTPUploadStrategy,
        "rsync_upload": RsyncUploadStrategy,
    }
    strategy_class = strategies.get(mode)
    if not strategy_class:
        raise ValueError(f"Unknown transfer mode: {mode}")
    return strategy_class(config, ssh_connection_pools)
