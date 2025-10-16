import logging
from typing import Dict

from .base import TorrentClient

def get_client(config_section: Dict[str, str]) -> TorrentClient:
    """
    Factory function to get a torrent client instance based on the config.
    """
    client_type_opt = config_section.get('type')
    if not client_type_opt or not client_type_opt.value:
        raise ValueError("Client 'type' not specified in the configuration section.")
    client_type = client_type_opt.value

    logging.info(f"Creating client of type: {client_type}")

    if client_type.lower() == 'qbittorrent':
        from .qbittorrent import QBittorrentClient
        return QBittorrentClient(config_section)
    elif client_type.lower() == 'deluge':
        from .deluge import DelugeClient
        return DelugeClient(config_section)
    # Future clients can be added here
    # elif client_type.lower() == 'transmission':
    #     from .transmission import TransmissionClient
    #     return TransmissionClient(config_section)
    else:
        raise ValueError(f"Unsupported client type: {client_type}")