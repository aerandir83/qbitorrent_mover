import unittest
from unittest.mock import MagicMock, patch
import configparser
import sys
import os
import time

# Ensure project root is in path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from clients.qbittorrent import QBittorrentClient

class TestQBittorrentPatience(unittest.TestCase):
    def setUp(self):
        self.config = configparser.ConfigParser()
        self.config.add_section('SOURCE_CLIENT')
        self.config['SOURCE_CLIENT'] = {
            'host': 'http://localhost',
            'port': '8080',
            'username': 'admin',
            'password': 'password'
        }
        self.client = QBittorrentClient(self.config['SOURCE_CLIENT'], 'Source')
        self.client.client = MagicMock() # manually inject mocked client
        self.ui_mock = MagicMock()

    @patch('time.sleep')
    @patch('time.time')
    def test_wait_for_recheck_stuck_with_grace_period(self, mock_time, mock_sleep):
        """
        Test that we do NOT fail if stuck at 0% progress within the grace period.
        Verifies that failure occurs only after grace_period + stuck_timeout.
        """
        # Robust Time Mocking
        self.cur_time = 1000.0
        mock_time.side_effect = lambda: self.cur_time

        def advance_time(sec):
            self.cur_time += sec
        mock_sleep.side_effect = advance_time

        # Mock Torrent Response
        t_zero = MagicMock(state='checkingUP', progress=0.0)
        self.client.client.torrents_info.return_value = [t_zero]

        result = self.client.wait_for_recheck(
            torrent_hash='hash1',
            ui_manager=self.ui_mock,
            stuck_timeout=5,
            stopped_timeout=10,
            grace_period=10
        )

        self.assertEqual(result, "FAILED_STUCK")

        # Verification of timing
        # Start: 1000
        # Grace expires at 1010.
        # Stuck countdown starts from last reset.
        # Last reset should be at 1008 (since 1008 < 1010).
        # So Stuck expires at 1008 + 5 = 1013.
        # We fail at first check > 1013.
        # Checks are at 2s intervals: 1000, 1002, ..., 1012, 1014.
        # 1014 > 1013. So failure at 1014.
        self.assertEqual(self.cur_time, 1014.0)

    @patch('time.sleep')
    @patch('time.time')
    def test_wait_for_recheck_stuck_past_grace_period(self, mock_time, mock_sleep):
        """
        Test that we FAIL if stuck at 0% progress AFTER the grace period.
        """
        self.cur_time = 1000.0
        mock_time.side_effect = lambda: self.cur_time

        def advance_time(sec):
            self.cur_time += sec
        mock_sleep.side_effect = advance_time

        t_zero = MagicMock(state='checkingUP', progress=0.0)
        self.client.client.torrents_info.return_value = [t_zero]

        result = self.client.wait_for_recheck(
            torrent_hash='hash1',
            ui_manager=self.ui_mock,
            stuck_timeout=5,
            stopped_timeout=10,
            grace_period=10
        )

        self.assertEqual(result, "FAILED_STUCK")
        # Should fail at same time as above logic
        self.assertEqual(self.cur_time, 1014.0)

    @patch('time.sleep')
    @patch('time.time')
    def test_api_safety_consecutive_failures(self, mock_time, mock_sleep):
        """
        Test that we need consecutive 'disappeared' responses to fail.
        """
        self.cur_time = 1000.0
        mock_time.side_effect = lambda: self.cur_time
        mock_sleep.side_effect = lambda s: None # Don't need to track time accumulation strictly

        t_checking = MagicMock(state='checkingUP', progress=0.5)
        t_done = MagicMock(state='uploading', progress=1.0)

        self.client.client.torrents_info.side_effect = [
            [],          # Disappeared (1st time)
            [t_checking], # Reappeared
            [t_done]      # Done
        ]

        result = self.client.wait_for_recheck(
            torrent_hash='hash1',
            ui_manager=self.ui_mock,
            stuck_timeout=5,
            stopped_timeout=10,
            grace_period=0
        )

        self.assertEqual(result, "SUCCESS")
        self.assertEqual(self.client.client.torrents_info.call_count, 3)

    @patch('time.sleep')
    @patch('time.time')
    def test_api_safety_confirmed_failure(self, mock_time, mock_sleep):
        """
        Test that we FAIL after consecutive 'disappeared' responses.
        """
        self.cur_time = 1000.0
        mock_time.side_effect = lambda: self.cur_time
        mock_sleep.side_effect = lambda s: None

        self.client.client.torrents_info.side_effect = [
            [], [], [], []
        ]

        result = self.client.wait_for_recheck(
            torrent_hash='hash1',
            ui_manager=self.ui_mock,
            stuck_timeout=5,
            stopped_timeout=10,
            grace_period=0
        )

        self.assertEqual(result, "FAILED_STATE")
        # It should fail after 3 checks
        self.assertEqual(self.client.client.torrents_info.call_count, 3)

if __name__ == '__main__':
    unittest.main()
