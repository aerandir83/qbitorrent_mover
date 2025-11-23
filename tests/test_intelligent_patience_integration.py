
import unittest
from unittest.mock import MagicMock, patch
import configparser
from torrent_mover import _post_transfer_actions
from clients.base import TorrentClient
from ui import BaseUIManager
from transfer_manager import FileTransferTracker

class TestGracePeriodIntegration(unittest.TestCase):

    def setUp(self):
        self.mock_torrent = MagicMock()
        self.mock_torrent.name = "test_torrent"
        self.mock_torrent.hash = "hash123"
        self.mock_torrent.category = "cat"

        self.mock_source_client = MagicMock(spec=TorrentClient)
        self.mock_dest_client = MagicMock(spec=TorrentClient)

        self.config = configparser.ConfigParser()
        self.config.add_section('SETTINGS')
        self.config.add_section('DESTINATION_CLIENT')
        self.config.add_section('SOURCE_CLIENT')

        # Set up default config
        self.config['SETTINGS']['transfer_mode'] = 'sftp'
        self.config['SETTINGS']['recheck_stuck_timeout'] = '60'
        self.config['SETTINGS']['recheck_stopped_timeout'] = '15'
        self.config['SETTINGS']['recheck_grace_period'] = '99' # Distinct value to verify

        self.mock_ui = MagicMock(spec=BaseUIManager)
        self.mock_file_tracker = MagicMock(spec=FileTransferTracker)
        self.mock_ssh_pools = {}

        # Mock dest client response
        self.mock_dest_client.wait_for_recheck.return_value = "SUCCESS"
        self.mock_dest_client.get_torrent_info.return_value = self.mock_torrent

    def test_grace_period_passed_to_wait_for_recheck(self):
        """Verify that recheck_grace_period from config is passed to wait_for_recheck."""

        _post_transfer_actions(
            torrent=self.mock_torrent,
            source_client=self.mock_source_client,
            destination_client=self.mock_dest_client,
            config=self.config,
            tracker_rules={},
            ssh_connection_pools=self.mock_ssh_pools,
            dest_content_path="/tmp/dest",
            destination_save_path="/tmp/save",
            transfer_executed=True,
            dry_run=False,
            test_run=False,
            file_tracker=self.mock_file_tracker,
            transfer_mode='sftp',
            all_files=[],
            ui=self.mock_ui,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock()
        )

        # Verify wait_for_recheck was called with correct grace_period
        self.mock_dest_client.wait_for_recheck.assert_called_once()
        call_args = self.mock_dest_client.wait_for_recheck.call_args
        self.assertEqual(call_args.kwargs['grace_period'], 99)
        self.assertEqual(call_args.kwargs['stuck_timeout'], 60)
        self.assertEqual(call_args.kwargs['stopped_timeout'], 15)

    @patch('torrent_mover._execute_transfer')
    @patch('torrent_mover.get_transfer_strategy')
    def test_grace_period_passed_to_final_recheck_after_repair(self, mock_get_strategy, mock_execute_transfer):
        """Verify grace period is passed to the second recheck call during repair."""

        # Setup first recheck to fail with FAILED_STATE (triggers repair)
        # Setup second recheck to succeed
        self.mock_dest_client.wait_for_recheck.side_effect = ["FAILED_STATE", "SUCCESS"]

        # Mock strategy to support correction
        mock_strategy = MagicMock()
        mock_strategy.supports_delta_correction.return_value = True
        mock_get_strategy.return_value = mock_strategy

        # Mock execute transfer success
        mock_execute_transfer.return_value = True

        _post_transfer_actions(
            torrent=self.mock_torrent,
            source_client=self.mock_source_client,
            destination_client=self.mock_dest_client,
            config=self.config,
            tracker_rules={},
            ssh_connection_pools=self.mock_ssh_pools,
            dest_content_path="/tmp/dest",
            destination_save_path="/tmp/save",
            transfer_executed=True,
            dry_run=False,
            test_run=False,
            file_tracker=self.mock_file_tracker,
            transfer_mode='sftp',
            all_files=[],
            ui=self.mock_ui,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock()
        )

        # Verify wait_for_recheck was called twice
        self.assertEqual(self.mock_dest_client.wait_for_recheck.call_count, 2)

        # Check second call arguments
        second_call_args = self.mock_dest_client.wait_for_recheck.call_args_list[1]
        self.assertEqual(second_call_args.kwargs['grace_period'], 99)

    def test_failed_stuck_logic(self):
        """Verify that FAILED_STUCK returns False and does NOT trigger delta-sync."""
        self.mock_dest_client.wait_for_recheck.return_value = "FAILED_STUCK"

        success, msg = _post_transfer_actions(
            torrent=self.mock_torrent,
            source_client=self.mock_source_client,
            destination_client=self.mock_dest_client,
            config=self.config,
            tracker_rules={},
            ssh_connection_pools=self.mock_ssh_pools,
            dest_content_path="/tmp/dest",
            destination_save_path="/tmp/save",
            transfer_executed=True,
            dry_run=False,
            test_run=False,
            file_tracker=self.mock_file_tracker,
            transfer_mode='sftp',
            all_files=[],
            ui=self.mock_ui,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock()
        )

        # Should pause torrent
        self.mock_dest_client.pause_torrent.assert_called_once_with(torrent_hash="hash123")

        # Should return False
        self.assertFalse(success)
        self.assertIn("Recheck failed (stuck)", msg)
        self.assertIn("no delta-sync", msg)

        # Should NOT call wait_for_recheck a second time
        self.assertEqual(self.mock_dest_client.wait_for_recheck.call_count, 1)


if __name__ == '__main__':
    unittest.main()
