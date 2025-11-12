"""Defines the strategy pattern for different file transfer methods.

This module provides a structured way to handle various transfer modes like SFTP
and rsync. It uses an abstract base class, `TransferStrategy`, to define a
common interface for all transfer methods. Concrete implementations of this
class are responsible for preparing the list of files to be transferred according
to the logic of that specific transfer type.

This approach allows the main application to easily switch between transfer
modes without changing the core orchestration logic. A factory function,
`get_transfer_strategy`, is provided to instantiate the correct strategy class
based on the user's configuration.
"""
from __future__ import annotations
import abc
import dataclasses
import logging
import configparser
from typing import Dict, List, TYPE_CHECKING, Tuple


if TYPE_CHECKING:
    from .ssh_manager import SSHConnectionPool
    from qbittorrentapi import TorrentDictionary

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class TransferFile:
    """A standardized representation of a file to be transferred.

    This dataclass holds the necessary information for a single file transfer,
    regardless of the transfer strategy being used.

    Attributes:
        source_path (str): The full path to the source file.
        dest_path (str): The full path where the file will be transferred.
        size (int): The size of the file in bytes.
        torrent_hash (str): The hash of the parent torrent for tracking purposes.
    """
    source_path: str
    dest_path: str
    size: int
    torrent_hash: str


class TransferStrategy(abc.ABC):
    """Abstract base class for defining different file transfer strategies.

    This class establishes the common interface that all transfer strategies must
    implement. This allows the main transfer logic to be decoupled from the
    specifics of how files are listed and prepared for each transfer mode.
    """

    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        """Initializes the transfer strategy.

        Args:
            config: The application's configuration object.
            ssh_connection_pools: A dictionary of available SSH connection pools.
        """
        self.config = config
        self.ssh_connection_pools = ssh_connection_pools

    @abc.abstractmethod
    def prepare_files(self, torrent: "TorrentDictionary", dest_path: str) -> List[TransferFile]:
        """Prepares a list of `TransferFile` objects for a given torrent.

        The implementation of this method will vary depending on the strategy.
        For example, an SFTP strategy will recursively list all files, while an
        rsync strategy might only return a single `TransferFile` for the root path.

        Args:
            torrent: The torrent object to be transferred.
            dest_path: The base destination path for the torrent's content.

        Returns:
            A list of `TransferFile` objects ready for transfer.
        """
        pass


class SFTPStrategy(TransferStrategy):
    """A transfer strategy for downloading files from a remote SFTP server to the local machine.

    This strategy connects to the source SFTP server, recursively lists all files
    if the torrent content is a directory, and creates a `TransferFile` object for
    each individual file. This allows for parallel file downloads.
    """

    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.pool = self.ssh_connection_pools[source_server_section]

    def prepare_files(self, torrent: "TorrentDictionary", dest_path: str) -> List[TransferFile]:
        """Lists all files on the SFTP server for a given torrent.

        Handles both single-file and multi-file (directory) torrents by checking
        the content path. For directories, it performs a recursive listing.

        Args:
            torrent: The torrent object.
            dest_path: The destination path for the torrent content.

        Returns:
            A list of `TransferFile` objects, one for each file to be downloaded.
        """
        from .ssh_manager import _get_all_files_recursive
        source_path = torrent.content_path.rstrip('/\\')
        file_list_tuples: List[Tuple[str, str, int]] = []

        with self.pool.get_connection() as (sftp, _ssh):
            try:
                stat_info = sftp.stat(source_path)
                is_dir = stat_info.st_mode & 0o40000  # S_ISDIR check

                if is_dir:
                    _get_all_files_recursive(sftp, source_path, dest_path, file_list_tuples)
                else:
                    file_list_tuples.append((source_path, dest_path, stat_info.st_size))
            except FileNotFoundError:
                logger.warning(f"Source path not found on SFTP server, skipping: {source_path}")

        return [
            TransferFile(source_path=src, dest_path=dest, size=size, torrent_hash=torrent.hash)
            for src, dest, size in file_list_tuples
        ]

class RsyncStrategy(TransferStrategy):
    """A transfer strategy for downloading content from a remote server using rsync.

    This strategy treats the entire torrent content (whether a single file or a
    directory) as a single transfer operation. It prepares just one `TransferFile`
    object representing the root content path. Parallel file processing is not
    supported, as rsync handles the entire transfer in one process.
    """

    def __init__(self, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]):
        super().__init__(config, ssh_connection_pools)
        source_server_section = self.config['SETTINGS']['source_server_section']
        self.pool = self.ssh_connection_pools[source_server_section]

    def prepare_files(self, torrent: "TorrentDictionary", dest_path: str) -> List[TransferFile]:
        """Prepares a single `TransferFile` representing the entire torrent content.

        Args:
            torrent: The torrent object.
            dest_path: The destination path for the torrent content.

        Returns:
            A list containing a single `TransferFile` for the entire torrent.
        """
        from .ssh_manager import batch_get_remote_sizes
        source_path = torrent.content_path.rstrip('/\\')
        with self.pool.get_connection() as (_sftp, ssh):
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

class SFTPUploadStrategy(SFTPStrategy):
    """A transfer strategy for server-to-server transfers using SFTP.

    This strategy is functionally similar to `SFTPStrategy` in how it prepares
    files. It recursively lists all files from the source SFTP server and creates
    a `TransferFile` for each one. The distinction is handled in the transfer
    execution logic, which will perform an SFTP download-to-cache-then-upload
    or a direct SFTP-to-SFTP stream.
    """
    # The implementation for prepare_files is identical to SFTPStrategy,
    # so we can inherit it directly without changes.
    pass


class RsyncUploadStrategy(RsyncStrategy):
    """A transfer strategy for server-to-server transfers using rsync.

    Similar to `RsyncStrategy`, this treats the entire torrent content as a single
    transfer operation. It prepares one `TransferFile` for the root content path.
    The transfer execution logic will handle the two-step process of rsyncing
    from source to a local cache, then rsyncing from the cache to the destination.
    """
    # The implementation for prepare_files is identical to RsyncStrategy,
    # so we can inherit it directly without changes.
    pass


def get_transfer_strategy(mode: str, config: configparser.ConfigParser, ssh_connection_pools: Dict[str, SSHConnectionPool]) -> TransferStrategy:
    """Factory function to instantiate the appropriate transfer strategy class.

    Based on the `mode` string from the configuration, this function returns an
    instance of the corresponding `TransferStrategy` subclass.

    Args:
        mode: The transfer mode string (e.g., "sftp", "rsync").
        config: The application's configuration object.
        ssh_connection_pools: A dictionary of available SSH connection pools.

    Returns:
        An instance of a `TransferStrategy` subclass.

    Raises:
        ValueError: If an unknown transfer mode is provided.
    """
    strategies: Dict[str, type[TransferStrategy]] = {
        "sftp": SFTPStrategy,
        "rsync": RsyncStrategy,
        "sftp_upload": SFTPUploadStrategy,
        "rsync_upload": RsyncUploadStrategy,
    }
    strategy_class = strategies.get(mode.lower())
    if not strategy_class:
        raise ValueError(f"Unknown transfer mode: {mode}")
    return strategy_class(config, ssh_connection_pools)
