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

    @patch('time.sleep')
    @patch('time.time')
    def test_recheck_returns_failed_state_on_stop(self, mock_time, mock_sleep):
        """
        Test that wait_for_recheck returns "FAILED_STATE" (triggering repair)
        when a torrent enters stoppedUP state with progress < 100%.
        """
        self.cur_time = 1000.0
        mock_time.side_effect = lambda: self.cur_time

        def advance_time(sec):
            self.cur_time += sec
        mock_sleep.side_effect = advance_time

        # Mock Torrent: Stopped with 99.8% progress
        t_stopped = MagicMock(state='stoppedUP', progress=0.998)

        # We need to simulate enough calls to exceed stopped_timeout (10s)
        # Poll interval is 2s.
        self.client.client.torrents_info.return_value = [t_stopped]

        result = self.client.wait_for_recheck(
            torrent_hash='hash1',
            ui_manager=self.ui_mock,
            stuck_timeout=5,
            stopped_timeout=10,
            grace_period=0
        )

        self.assertEqual(result, "FAILED_STATE")
        # Time check: Initial detection + wait > 10s.
        # It starts at 1000. Detects stopped at 1000.
        # Loops: 1002, 1004, 1006, 1008, 1010 (elapsed 10, not > 10), 1012 (elapsed 12 > 10).
        self.assertEqual(self.cur_time, 1012.0)

    @patch('time.sleep')
    @patch('time.time')
    def test_wait_for_recheck_grace_period(self, mock_time, mock_sleep):
        """
        Test Grace Period Scenario:
        1. Torrent is checking at 0.0%.
        2. Ensure function does not return FAILED_STUCK within grace_period.
        3. Advance time beyond grace_period + stuck_timeout -> FAILED_STUCK.
        """
        self.cur_time = 1000.0
        mock_time.side_effect = lambda: self.cur_time

        def advance_time(sec):
            self.cur_time += sec
        mock_sleep.side_effect = advance_time

        # Mock Torrent: Checking at 0.0%
        t_zero = MagicMock(state='checkingDL', progress=0.0)
        self.client.client.torrents_info.return_value = [t_zero]

        # stuck_timeout=5, grace_period=10
        # Expected failure time: 1000 + 10 (grace) + 5 (stuck) = 1015?
        # Let's trace logic:
        # T=1000. Start. Last progress=1000.
        # ...
        # T=1008. Elapsed since start=8 < 10. Reset last_progress=1008.
        # T=1010. Elapsed since start=10. Not < 10. Do NOT reset.
        # T=1012. Elapsed stuck = 1012 - 1008 = 4 < 5.
        # T=1014. Elapsed stuck = 1014 - 1008 = 6 > 5. FAIL.
        # So failure at 1014.

        result = self.client.wait_for_recheck(
            torrent_hash='hash1',
            ui_manager=self.ui_mock,
            stuck_timeout=5,
            stopped_timeout=10,
            grace_period=10
        )

        self.assertEqual(result, "FAILED_STUCK")
        self.assertEqual(self.cur_time, 1014.0)

if __name__ == '__main__':
    unittest.main()
