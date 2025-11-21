import configparser
import logging
from typing import Optional
from clients.base import TorrentClient

def get_client(config_section: configparser.SectionProxy, client_type: str, client_name: str) -> Optional[TorrentClient]:
    """
    Factory function to get a torrent client instance.

    Args:
        config_section: Configuration section for the client.
        client_type: Type of the client (e.g., 'qbittorrent').
        client_name: Name of the client instance.

    Returns:
        An instance of TorrentClient, or None if the client type is unknown.
    """
    if client_type.lower() == 'qbittorrent':
        # TODO: Implement qBittorrent client creation in the next task
        return None
    else:
        logging.error(f"Unknown client type: {client_type}")
        return None
