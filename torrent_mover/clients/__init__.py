from torrent_mover.clients.qbittorrent import QBittorrentClient
from torrent_mover.clients.deluge import DelugeClient

CLIENTS = {
    "qbittorrent": QBittorrentClient,
    "deluge": DelugeClient,
}

def get_client(client_name, config_section):
    """
    Factory function to get a torrent client instance.
    """
    client_class = CLIENTS.get(client_name.lower())
    if not client_class:
        raise ValueError(f"Unknown client: {client_name}")
    return client_class(config_section)
