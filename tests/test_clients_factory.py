import unittest
from unittest.mock import MagicMock, patch
import configparser
from clients.factory import get_client
from clients.qbittorrent import QBittorrentClient

class TestClientFactory(unittest.TestCase):
    def setUp(self):
        self.config_section = MagicMock(spec=configparser.SectionProxy)

    @patch('clients.factory.logging.error')
    def test_get_client_qbittorrent(self, mock_logging_error):
        client = get_client(self.config_section, 'qbittorrent', 'test_client')
        self.assertIsInstance(client, QBittorrentClient)
        mock_logging_error.assert_not_called()

    @patch('clients.factory.logging.error')
    def test_get_client_unknown(self, mock_logging_error):
        # Should return None and log error
        client = get_client(self.config_section, 'unknown_client', 'test_client')
        self.assertIsNone(client)
        mock_logging_error.assert_called_with('Unknown client type: unknown_client')
