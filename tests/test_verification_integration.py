import unittest
from unittest.mock import MagicMock, patch
import configparser
import sys
import os

# Ensure project root is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from torrent_mover import _post_transfer_actions
from tests.mocks.mock_qbittorrent import MockTorrent

class TestVerificationIntegration(unittest.TestCase):

    def setUp(self):
        self.mock_config = configparser.ConfigParser()
        self.mock_config.add_section('SETTINGS')
        self.mock_config['SETTINGS'] = {
            'transfer_mode': 'rsync',
            'manual_review_category': 'review-failed',
            'recheck_stuck_timeout': '123',
            'recheck_stopped_timeout': '456',
            'recheck_grace_period': '789'
        }
        self.mock_config.add_section('DESTINATION_PATHS')
        self.mock_config['DESTINATION_PATHS'] = {
            'destination_path': '/dest',
            'remote_destination_path': '/dest'
        }
        self.mock_config.add_section('DESTINATION_CLIENT')
        self.mock_config['DESTINATION_CLIENT'] = {
            'add_torrents_paused': 'true',
            'start_torrents_after_recheck': 'true',
        }
        self.mock_config.add_section('SOURCE_CLIENT')
        self.mock_config['SOURCE_CLIENT'] = {
            'delete_after_transfer': 'true'
        }
        self.mock_config.add_section('SOURCE_SERVER')
        self.mock_config.add_section('DESTINATION_SERVER')

        self.mock_ui = MagicMock()
        self.mock_ui.log = MagicMock()
        self.mock_ui.pet_watchdog = MagicMock()

    @patch('torrent_mover.change_ownership', return_value=True)
    def test_post_transfer_calls_wait_for_recheck_with_correct_args(self, mock_chown):
        """
        Verify that wait_for_recheck is called with the correct arguments from the configuration.
        """
        torrent = MockTorrent(name="Test", hash_="hash123", content_path="/src", save_path="/src")
        # Ensure hash attribute matches what _post_transfer_actions expects if it uses property
        torrent.hash = "hash123"

        dest_client = MagicMock()
        # Mock return value to avoid side effects
        dest_client.wait_for_recheck.return_value = "SUCCESS"
        dest_client.get_torrent_info.return_value = torrent

        source_client = MagicMock()

        # Call function
        _post_transfer_actions(
            torrent=torrent,
            source_client=source_client,
            destination_client=dest_client,
            config=self.mock_config,
            tracker_rules={},
            ssh_connection_pools={},
            dest_content_path="/dest",
            destination_save_path="/dest",
            transfer_executed=True,
            dry_run=False,
            test_run=False,
            file_tracker=MagicMock(),
            transfer_mode="rsync",
            all_files=[],
            ui=self.mock_ui,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock()
        )

        # Verify call arguments
        dest_client.wait_for_recheck.assert_called_with(
            torrent_hash="hash123",
            ui_manager=self.mock_ui,
            stuck_timeout=123,
            stopped_timeout=456,
            grace_period=789,
            dry_run=False
        )
