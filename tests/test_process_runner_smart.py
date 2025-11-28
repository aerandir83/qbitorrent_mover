import unittest
from unittest.mock import MagicMock, patch, ANY
import sys
import os

# Ensure we can import process_runner
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from process_runner import execute_streaming_command
from utils import RemoteTransferError

class TestProcessRunnerSmart(unittest.TestCase):

    def setUp(self):
        self.log_transfer = MagicMock()
        self.update_progress = MagicMock()
        self.heartbeat = MagicMock()
        self.torrent_hash = "abc123hash"
        self.total_size = 1024 * 1024 * 100 # 100MB

    @patch('process_runner.fcntl.fcntl')
    @patch('process_runner.select.select')
    @patch('process_runner.os.read')
    @patch('process_runner.subprocess.Popen')
    @patch('process_runner.time.time')
    def test_smart_progress_success(self, mock_time, mock_popen, mock_read, mock_select, mock_fcntl):
        """
        Test Case 1 (Success): Mock a subprocess that outputs "progress" lines every 1 second for 5 seconds.
        Set timeout_seconds=2. Ensure it does not kill the process (because output resets the timer).
        """
        # Setup time to advance
        start_time = 1000.0
        current_time = start_time

        def time_side_effect():
            nonlocal current_time
            t = current_time
            # We verify if we should advance time?
            # If we advance time on every call, logging calls will also advance time, which is messy.
            # But for the purpose of the test, strictly increasing time is fine.
            current_time += 0.1
            return t

        mock_time.side_effect = time_side_effect

        # Mock process
        process = MagicMock()
        process.stdout.fileno.return_value = 10
        process.stderr.read.return_value = b""
        mock_popen.return_value = process

        # Poll sequence
        poll_count = 0
        def poll_side_effect():
            nonlocal poll_count
            poll_count += 1
            if poll_count > 10:
                return 0 # Finished
            return None # Alive
        process.poll.side_effect = poll_side_effect
        process.returncode = 0

        # Select sequence
        # Alternating ready/not ready
        select_count = 0
        def select_side_effect(*args, **kwargs):
            nonlocal select_count
            select_count += 1
            if select_count % 2 != 0:
                return ([process.stdout], [], []) # Ready
            return ([], [], []) # Not ready

        mock_select.side_effect = select_side_effect

        # Read sequence (bytes)
        # Simulate real rsync output: 10,500,000 bytes, 10%, 2.1MB/s, 0:00:40
        rsync_line = b"   10,500,000  10%  2.1MB/s    0:00:40\n"
        mock_read.return_value = rsync_line
        mock_fcntl.return_value = 0

        success = execute_streaming_command(
            ["cmd"],
            self.torrent_hash,
            self.total_size,
            self.log_transfer,
            self.update_progress,
            heartbeat_callback=self.heartbeat,
            timeout_seconds=5
        )

        self.assertTrue(success)
        # Check heartbeat called
        self.assertTrue(self.heartbeat.called)

        # Check parsing happened
        self.update_progress.assert_called()
        # Check process wasn't killed
        process.kill.assert_not_called()
        process.terminate.assert_not_called()

    @patch('process_runner.fcntl.fcntl')
    @patch('process_runner.select.select')
    @patch('process_runner.os.read')
    @patch('process_runner.subprocess.Popen')
    @patch('process_runner.time.time')
    def test_smart_progress_timeout(self, mock_time, mock_popen, mock_read, mock_select, mock_fcntl):
        """
        Test Case 2 (Failure): Mock a subprocess that hangs (outputs nothing) for 3 seconds.
        Set timeout_seconds=1. Ensure it kills the process and raises an exception.
        """
        # Setup time
        start_time = 1000.0
        current_time = start_time

        def time_side_effect():
            nonlocal current_time
            t = current_time
            # We want to force a timeout eventually.
            # The code checks: if time.time() - last_output_time > timeout_seconds
            # last_output_time is initialized to start_time.
            # We need current_time to increase enough.
            current_time += 0.5
            return t

        mock_time.side_effect = time_side_effect

        process = MagicMock()
        process.stdout.fileno.return_value = 10
        mock_popen.return_value = process

        process.poll.return_value = None # Always alive

        mock_select.return_value = ([], [], []) # Always timeout (no data)
        mock_fcntl.return_value = 0

        with self.assertRaises(RemoteTransferError) as cm:
            execute_streaming_command(
                ["cmd"],
                self.torrent_hash,
                self.total_size,
                self.log_transfer,
                self.update_progress,
                heartbeat_callback=self.heartbeat,
                timeout_seconds=1
            )

        self.assertIn("timed out", str(cm.exception))
        # Verify cleanup
        self.assertTrue(process.terminate.called or process.kill.called)
