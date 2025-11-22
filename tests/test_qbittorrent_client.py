import unittest
from unittest.mock import MagicMock, patch
import configparser
import os
import sys

# Ensure project root is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from clients.factory import get_client
from clients.qbittorrent import QBittorrentClient

class TestQBittorrentClient(unittest.TestCase):
    def setUp(self):
        self.config = configparser.ConfigParser()
        self.config.add_section('SOURCE_CLIENT')
        self.config['SOURCE_CLIENT'] = {
            'host': 'http://localhost',
            'port': '8080',
            'username': 'admin',
            'password': 'password'
        }
        self.client_section = self.config['SOURCE_CLIENT']

    def test_factory_creation(self):
        client = get_client(self.client_section, 'qbittorrent', 'Source')
        self.assertIsInstance(client, QBittorrentClient)
        self.assertEqual(client.client_name, 'Source')

    @patch('clients.qbittorrent.qbittorrentapi.Client')
    def test_connect(self, mock_qbit_client):
        client = get_client(self.client_section, 'qbittorrent', 'Source')

        # Mock the qbittorrent client instance
        mock_instance = mock_qbit_client.return_value
        mock_instance.app.version = "v4.3.9"

        connected = client.connect()
        self.assertTrue(connected)

        # Verify correct args passed to qbittorrentapi.Client
        # Note: Timeouts.SSH_CONNECT is 10 by default in my implementation
        mock_qbit_client.assert_called_with(
            host='http://localhost',
            port=8080,
            username='admin',
            password='password',
            VERIFY_WEBUI_CERTIFICATE=True,
            REQUESTS_ARGS={'timeout': 10}
        )
        mock_instance.auth_log_in.assert_called_once()

    def test_wait_for_recheck_dry_run(self):
        client = get_client(self.client_section, 'qbittorrent', 'Source')
        # No connect needed for dry_run check

        ui_mock = MagicMock()
        result = client.wait_for_recheck("hash123", ui_mock, 10, 10, dry_run=True)
        self.assertEqual(result, "SUCCESS")

    @patch('clients.qbittorrent.qbittorrentapi.Client')
    def test_get_eligible_torrents_no_threshold(self, mock_qbit_client):
        client = get_client(self.client_section, 'qbittorrent', 'Source')
        client.connect()

        mock_instance = client.client
        mock_torrent = MagicMock()
        mock_torrent.name = "Linux ISO"
        mock_instance.torrents_info.return_value = [mock_torrent]

        torrents = client.get_eligible_torrents("linux_isos")

        mock_instance.torrents_info.assert_called_with(category="linux_isos", status_filter='completed')
        self.assertEqual(len(torrents), 1)
        self.assertEqual(torrents[0].name, "Linux ISO")

if __name__ == '__main__':
    unittest.main()
