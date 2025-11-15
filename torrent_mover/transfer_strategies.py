from __future__ import annotations

import abc
import configparser
import dataclasses
import logging
import os
import shlex
import argparse
from typing import cast, Dict, List, Optional, TYPE_CHECKING, Tuple


if TYPE_CHECKING:
    import qbittorrentapi
    from .ssh_manager import SSHConnectionPool
    from .ui import BaseUIManager
    from .transfer_manager import FileTransferTracker

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

    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        self.config = config
        self.ssh_connection_pools = ssh_connection_pools
        self.transfer_mode = config['SETTINGS'].get('transfer_mode', 'sftp').lower()

    @abc.abstractmethod
    def pre_transfer_check(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        total_size: int,
        args: argparse.Namespace,
    ) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
        """
        Performs setup tasks before a torrent transfer begins.
        Should return a tuple of:
        - status_code (str)
        - message (str)
        - source_content_path (Optional[str])
        - dest_content_path (Optional[str])
        - destination_save_path (Optional[str])
        """
        pass

    @abc.abstractmethod
    def prepare_files(self, torrent: "qbittorrentapi.TorrentDictionary", dest_path: str) -> List[TransferFile]:
        """Prepare a list of files to be transferred for a given torrent."""
        pass

    @abc.abstractmethod
    def execute_transfer(
        self,
        files: List[TransferFile],
        torrent: "qbittorrentapi.TorrentDictionary",
        ui: "BaseUIManager",
        file_tracker: "FileTransferTracker",
        dry_run: bool,
    ) -> bool:
        """Executes the file transfer for a given list of files."""
        pass

    @abc.abstractmethod
    def post_transfer_actions(
        self,
        torrent: "qbittorrentapi.TorrentDictionary",
        source_qbit: "qbittorrentapi.Client",
        destination_qbit: Optional["qbittorrentapi.Client"],
        tracker_rules: Dict[str, str],
        dest_content_path: str,
        destination_save_path: str,
        transfer_executed: bool,
        dry_run: bool,
        test_run: bool,
        file_tracker: "FileTransferTracker",
        all_files: List[TransferFile],
        ui: "BaseUIManager",
    ) -> Tuple[bool, str]:
        """Manages tasks after the file transfer is complete."""
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
        Handles both single-file and multi-file (directory) torrents.
        """
        from .ssh_manager import _get_all_files_recursive
        source_path = torrent.content_path.rstrip('/\\')
        file_list_tuples: List[Tuple[str, str, int]] = []

        with self.pool.get_connection() as (sftp, ssh):
            try:
                stat_info = sftp.stat(source_path)
                is_dir = stat_info.st_mode & 0o40000

                if is_dir:
                    # It's a directory, recurse
                    _get_all_files_recursive(sftp, source_path, dest_path, file_list_tuples)
                else:
                    # It's a single file.
                    # The dest_path provided by the caller is already the full destination file path.
                    file_list_tuples.append((source_path, dest_path, stat_info.st_size))

            except FileNotFoundError:
                logger.warning(f"Source path not found, skipping: {source_path}")
                # Return empty list

        return [
            TransferFile(
                source_path=src,
                dest_path=dest,
                size=size,
                torrent_hash=torrent.hash
            ) for src, dest, size in file_list_tuples
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
        file_list_tuples: List[Tuple[str, str, int]] = []

        with self.pool.get_connection() as (sftp, ssh):
            try:
                stat_info = sftp.stat(source_path)
                is_dir = stat_info.st_mode & 0o40000

                if is_dir:
                    # It's a directory, recurse
                    _get_all_files_recursive(sftp, source_path, dest_path, file_list_tuples)
                else:
                    # It's a single file.
                    # The dest_path provided by the caller is already the full destination file path.
                    file_list_tuples.append((source_path, dest_path, stat_info.st_size))

            except FileNotFoundError:
                logger.warning(f"Source path not found, skipping: {source_path}")
                # Return empty list

        return [
            TransferFile(
                source_path=src,
                dest_path=dest,
                size=size,
                torrent_hash=torrent.hash
            ) for src, dest, size in file_list_tuples
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
