import qbittorrentapi
from torrent_mover.utils import retry
from torrent_mover.clients.base import SimpleTorrent, TorrentClient

class QBittorrentClient(TorrentClient):
    """
    A qBittorrent client implementation.
    """

    def __init__(self, config_section):
        super().__init__(config_section)
        self.client = None

    @retry(tries=2, delay=5)
    def connect(self):
        """Connects to the qBittorrent client."""
        host = self.config["host"]
        port = self.config.getint("port")
        username = self.config["username"]
        password = self.config["password"]
        verify_cert = self.config.getboolean("verify_cert", fallback=True)

        self.client = qbittorrentapi.Client(
            host=host,
            port=port,
            username=username,
            password=password,
            VERIFY_WEBUI_CERTIFICATE=verify_cert,
            REQUESTS_ARGS={"timeout": 10},
        )
        self.client.auth_log_in()

    def get_app_version(self):
        """Returns the version of the qBittorrent application."""
        return self.client.app.version

    def get_torrents(self, **kwargs):
        """
        Returns a list of torrents.
        The torrents are returned as a list of SimpleTorrent objects.
        """
        torrents = self.client.torrents_info(**kwargs)
        return [
            SimpleTorrent(
                hash=t.hash,
                name=t.name,
                category=t.category,
                size=t.size,
                state=t.state,
                progress=t.progress,
                added_on=t.added_on,
                content_path=t.content_path,
            )
            for t in torrents
        ]

    def export_torrent(self, torrent_hash):
        """Returns the content of a .torrent file for the given hash."""
        return self.client.torrents_export(torrent_hash=torrent_hash)

    def get_magnet_uri(self, torrent_hash):
        """Returns the magnet URI for the given torrent hash."""
        return self.client.torrents_info(torrent_hashes=torrent_hash)[0].magnet_uri

    def add_torrent(self, torrent_data, **kwargs):
        """Adds a new torrent to the client."""
        if isinstance(torrent_data, bytes):
            self.client.torrents_add(torrent_files=torrent_data, **kwargs)
        elif isinstance(torrent_data, str) and torrent_data.startswith("magnet:"):
            self.client.torrents_add(urls=torrent_data, **kwargs)

    def recheck_torrent(self, torrent_hashes):
        """Rechecks the specified torrents."""
        self.client.torrents_recheck(torrent_hashes=torrent_hashes)

    def resume_torrent(self, torrent_hashes):
        """Resumes the specified torrents."""
        self.client.torrents_resume(torrent_hashes=torrent_hashes)

    def pause_torrent(self, torrent_hashes):
        """Pauses the specified torrents."""
        self.client.torrents_pause(torrent_hashes=torrent_hashes)

    def delete_torrent(self, torrent_hashes, delete_files):
        """Deletes the specified torrents."""
        self.client.torrents_delete(
            torrent_hashes=torrent_hashes, delete_files=delete_files
        )

    def set_category(self, torrent_hashes, category):
        """Sets the category for the specified torrents."""
        self.client.torrents_set_category(
            torrent_hashes=torrent_hashes, category=category
        )

    def get_trackers(self, torrent_hash):
        """Returns the trackers for a specific torrent."""
        return self.client.torrents_trackers(torrent_hash=torrent_hash)

    def get_categories(self):
        """Returns a list of available categories."""
        return list(self.client.torrent_categories.categories.keys())
