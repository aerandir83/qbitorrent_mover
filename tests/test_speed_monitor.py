import unittest
from unittest.mock import MagicMock, patch
from speed_monitor import SpeedMonitor

class TestSpeedMonitor(unittest.TestCase):
    def setUp(self):
        self.monitor = None

    def tearDown(self):
        if self.monitor and self.monitor.is_alive():
            self.monitor.stop_monitoring()
            self.monitor.join(timeout=1)

    @patch('speed_monitor.time')
    @patch('speed_monitor.sleep')
    def test_basic_math(self, mock_sleep, mock_time):
        """Test 1: Basic Math - Verify speed calculation."""
        # time() called:
        # 1. Initial last_time (1000.0)
        # 2. Loop 1 current_time (1001.0)
        mock_time.side_effect = [1000.0, 1001.0]

        # size_fetcher called:
        # 1. Initial last_size (100)
        # 2. Loop 1 current_size (200)
        mock_size_fetcher = MagicMock(side_effect=[100, 200])

        self.monitor = SpeedMonitor(file_path="dummy", size_fetcher=mock_size_fetcher, interval=1.0)

        # Stop after 1 loop iteration.
        # sleep is called at the start of the loop.
        # Call 1: Start of Loop 1.
        # Call 2: Start of Loop 2 -> Trigger stop.
        def sleep_side_effect(*args):
            if mock_sleep.call_count >= 2:
                self.monitor._running = False

        mock_sleep.side_effect = sleep_side_effect

        self.monitor.start_monitoring()
        self.monitor.join()

        status = self.monitor.get_status()
        # Delta size = 200 - 100 = 100
        # Delta time = 1001 - 1000 = 1
        # Speed = 100.0
        self.assertAlmostEqual(status['current_speed'], 100.0)
        self.assertEqual(len(status['history']), 1)
        self.assertEqual(status['history'][0], 100.0)

    @patch('speed_monitor.time')
    @patch('speed_monitor.sleep')
    def test_smoothing(self, mock_sleep, mock_time):
        """Test 2: Smoothing - Verify average speed calculation."""
        # Mock inputs: 100 -> 200 (Delta 100), then 200 -> 200 (Delta 0).
        # Assert current_speed is 50.0 (Average of 100 and 0).

        # Time calls:
        # 1. Initial (1000)
        # 2. Loop 1 (1001) -> dt=1
        # 3. Loop 2 (1002) -> dt=1
        mock_time.side_effect = [1000.0, 1001.0, 1002.0]

        # Size calls:
        # 1. Initial (100)
        # 2. Loop 1 (200) -> ds=100. Speed=100.
        # 3. Loop 2 (200) -> ds=0. Speed=0.
        mock_size_fetcher = MagicMock(side_effect=[100, 200, 200])

        self.monitor = SpeedMonitor(file_path="dummy", size_fetcher=mock_size_fetcher)

        # Stop after 2 loops (so 3 sleep calls)
        def sleep_side_effect(*args):
            if mock_sleep.call_count >= 3:
                self.monitor._running = False
        mock_sleep.side_effect = sleep_side_effect

        self.monitor.start_monitoring()
        self.monitor.join()

        status = self.monitor.get_status()
        # Average of 100 and 0 is 50.
        self.assertAlmostEqual(status['current_speed'], 50.0)
        self.assertEqual(list(status['history']), [100.0, 0.0])

    @patch('speed_monitor.time')
    @patch('speed_monitor.sleep')
    def test_file_reset(self, mock_sleep, mock_time):
        """Test 3: File Reset - Verify handling of file replacement/shrink."""
        # Mock inputs: 500 -> 100 (Delta -400).
        # Assert this sample is ignored.

        mock_time.side_effect = [1000.0, 1001.0, 1002.0]
        # Initial: 500
        # Loop 1: 100 (Reset detected, ignored)
        # Loop 2: 150 (Delta = 150 - 100 = 50) -> Speed 50.
        mock_size_fetcher = MagicMock(side_effect=[500, 100, 150])

        self.monitor = SpeedMonitor(file_path="dummy", size_fetcher=mock_size_fetcher)

        # Stop after 2 loops (3 sleeps)
        def sleep_side_effect(*args):
            if mock_sleep.call_count >= 3:
                self.monitor._running = False
        mock_sleep.side_effect = sleep_side_effect

        self.monitor.start_monitoring()
        self.monitor.join()

        status = self.monitor.get_status()
        # First sample ignored. Second sample: size 150, last_size 100 -> delta 50. dt 1. Speed 50.
        self.assertEqual(list(status['history']), [50.0])
        self.assertAlmostEqual(status['current_speed'], 50.0)

    @patch('speed_monitor.time')
    @patch('speed_monitor.sleep')
    def test_error_handling(self, mock_sleep, mock_time):
        """Test 4: Error Handling - Verify FileNotFoundError handling."""
        # Mock size_fetcher raising FileNotFoundError
        # Assert speed 0.

        mock_time.side_effect = [1000.0, 1001.0, 1002.0] # Added extra time for safety as exception handler might call it

        # Initial: 100
        # Loop 1: FileNotFoundError
        mock_size_fetcher = MagicMock(side_effect=[100, FileNotFoundError("File gone")])

        self.monitor = SpeedMonitor(file_path="dummy", size_fetcher=mock_size_fetcher)

        # Stop after 1 loop (2 sleeps)
        def sleep_side_effect(*args):
            if mock_sleep.call_count >= 2:
                self.monitor._running = False
        mock_sleep.side_effect = sleep_side_effect

        self.monitor.start_monitoring()
        self.monitor.join()

        status = self.monitor.get_status()
        self.assertEqual(list(status['history']), [0.0])
        self.assertAlmostEqual(status['current_speed'], 0.0)
