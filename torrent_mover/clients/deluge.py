import tempfile
import os
from deluge_web_client import DelugeWebClient
from torrent_mover.utils import retry
from torrent_mover.clients.base import SimpleTorrent, TorrentClient

class DelugeClient(TorrentClient):
    """
    A Deluge client implementation.
    """

    def __init__(self, config_section):
        super().__init__(config_section)
        self.client = None

    @retry(tries=2, delay=5)
    def connect(self):
        """Connects to the Deluge client."""
        host = self.config["host"]
        port = self.config.getint("port")
        password = self.config["password"]

        # DelugeWebClient expects a full URL
        url = f"http://{host}:{port}"

        self.client = DelugeWebClient(url=url, password=password)
        self.client.login()

    def get_app_version(self):
        """Returns the version of the Deluge application."""
        # The deluge-web-client library does not directly expose the version.
        # We can get it via a raw RPC call if needed, but for now, we'll return a placeholder.
        return self.client.get_daemon_version()

    def get_torrents(self, **kwargs):
        """
        Returns a list of torrents.
        The torrents are returned as a list of SimpleTorrent objects.
        """
        # The Deluge API uses a different filtering mechanism than qBittorrent.
        # We will fetch all torrents and filter them locally.
        all_torrents = self.client.get_torrents_status()

        # Local filtering based on provided kwargs
        filtered_torrents = []
        for hash, t in all_torrents.items():
            match = True
            if "category" in kwargs and t.get("label") != kwargs["category"]:
                match = False
            if "status_filter" in kwargs and kwargs["status_filter"] == "completed" and t.get("progress") < 100:
                match = False

            if match:
                filtered_torrents.append(t)

        return [
            SimpleTorrent(
                hash=t["hash"],
                name=t["name"],
                category=t.get("label", ""), # Deluge uses 'label' for category
                size=t["total_size"],
                state=t["state"],
                progress=t["progress"] / 100.0, # Deluge progress is 0-100
                added_on=t["time_added"],
                content_path=t["save_path"], # This might just be the save path, not the full content path
            )
            for t in filtered_torrents
        ]

    def export_torrent(self, torrent_hash):
        """Returns the content of a .torrent file for the given hash. Not supported by Deluge Web API."""
        raise NotImplementedError("Exporting .torrent files is not supported by the Deluge Web API.")

    def get_magnet_uri(self, torrent_hash):
        """Returns the magnet URI for the given torrent hash."""
        return f"magnet:?xt=urn:btih:{torrent_hash}"

    def add_torrent(self, torrent_data, **kwargs):
        """Adds a new torrent to the client."""
        if isinstance(torrent_data, bytes):
            with tempfile.NamedTemporaryFile(delete=False, suffix=".torrent") as tmp:
                tmp.write(torrent_data)
                tmp_path = tmp.name

            try:
                self.client.upload_torrent(
                    torrent_path=tmp_path,
                    add_paused=kwargs.get("is_paused", False),
                    save_directory=kwargs.get("save_path"),
                    label=kwargs.get("category")
                )
            finally:
                os.remove(tmp_path)
        elif isinstance(torrent_data, str) and torrent_data.startswith("magnet:"):
            self.client.add_torrent_magnet(
                magnet_uri=torrent_data,
                add_paused=kwargs.get("is_paused", False),
                save_path=kwargs.get("save_path"),
            )
            # Deluge doesn't allow setting a label/category when adding via magnet.
            # This would need to be a separate call after the torrent is added and its hash is known.
            # For now, we'll have to rely on the tracker-based categorization to handle this.


    def recheck_torrent(self, torrent_hashes):
        """Rechecks the specified torrents."""
        self.client.force_recheck(torrent_hashes)

    def resume_torrent(self, torrent_hashes):
        """Resumes the specified torrents."""
        self.client.resume_torrent(torrent_hashes)

    def pause_torrent(self, torrent_hashes):
        """Pauses the specified torrents."""
        self.client.pause_torrent(torrent_hashes)

    def delete_torrent(self, torrent_hashes, delete_files):
        """Deletes the specified torrents."""
        self.client.remove_torrent(torrent_hashes, remove_data=delete_files)

    def set_category(self, torrent_hashes, category):
        """Sets the category for the specified torrents."""
        self.client.set_torrent_label(torrent_hashes, category)

    def get_trackers(self, torrent_hash):
        """Returns the trackers for a specific torrent."""
        return self.client.get_torrent_status(torrent_hash).get("trackers", [])

    def get_categories(self):
        """Returns a list of available categories."""
        return self.client.get_labels()
