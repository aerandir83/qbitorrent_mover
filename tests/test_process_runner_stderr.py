import unittest
from unittest.mock import MagicMock, patch, ANY
import sys
import os

# Ensure we can import process_runner
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from process_runner import execute_streaming_command
from utils import RemoteTransferError

class TestProcessRunnerStderr(unittest.TestCase):

    def setUp(self):
        self.log_transfer = MagicMock()
        self.update_progress = MagicMock()
        self.heartbeat = MagicMock()
        self.torrent_hash = "abc123hash"
        self.total_size = 1024 * 1024 * 100 # 100MB

    @patch('process_runner.logger')
    @patch('process_runner.fcntl.fcntl')
    @patch('process_runner.select.select')
    @patch('process_runner.os.read')
    @patch('process_runner.subprocess.Popen')
    @patch('process_runner.time.time')
    def test_stderr_streaming_and_raw_stdout(self, mock_time, mock_popen, mock_read, mock_select, mock_fcntl, mock_logger):
        """
        Test that stderr is monitored, read, and logged in real-time, and stdout is logged raw.
        """
        # Setup time
        start_time = 1000.0
        current_time = start_time
        def time_side_effect():
            nonlocal current_time
            current_time += 0.1
            return current_time
        mock_time.side_effect = time_side_effect

        # Mock process
        process = MagicMock()
        process.stdout.fileno.return_value = 10
        process.stderr.fileno.return_value = 11
        mock_popen.return_value = process

        # Poll sequence
        # We'll let it run a few loops then fail
        poll_count = 0
        def poll_side_effect():
            nonlocal poll_count
            poll_count += 1
            if poll_count > 5:
                return 1 # Failed
            return None # Alive
        process.poll.side_effect = poll_side_effect
        process.returncode = 1

        # Select sequence
        select_count = 0
        def select_side_effect(rlist, wlist, xlist, timeout):
            # CHECK 1: stderr must be in rlist
            if process.stderr not in rlist:
                raise AssertionError("process.stderr was NOT included in select() rlist!")

            nonlocal select_count
            select_count += 1
            # Return stdout ready, then stderr ready
            if select_count == 1:
                return ([process.stdout], [], [])
            elif select_count == 2:
                return ([process.stderr], [], [])
            return ([], [], [])

        mock_select.side_effect = select_side_effect

        # Read sequence
        def read_side_effect(fd, size):
            if fd == 10: # stdout
                return b"stdout_chunk"
            elif fd == 11: # stderr
                return b"stderr_chunk"
            return b""
        mock_read.side_effect = read_side_effect

        mock_fcntl.return_value = 0

        # Run
        with self.assertRaises(RemoteTransferError) as cm:
            execute_streaming_command(
                ["cmd"],
                self.torrent_hash,
                self.total_size,
                self.log_transfer,
                self.update_progress,
                heartbeat_callback=self.heartbeat,
                timeout_seconds=5
            )

        # CHECK 2: Exception message should contain the accumulated stderr
        self.assertIn("stderr_chunk", str(cm.exception))

        # CHECK 3: Verify fcntl called for stderr (fd 11)
        # We need to see fcntl(11, F_SETFL, ...)
        stderr_fcntl_found = False
        for call_args in mock_fcntl.call_args_list:
            # call_args is (args, kwargs)
            # args might be (fd, cmd) or (fd, cmd, arg)
            if call_args[0][0] == 11:
                stderr_fcntl_found = True
                break
        self.assertTrue(stderr_fcntl_found, "fcntl was not called for stderr (fd 11)")

        # CHECK 4: Verify logger calls for real-time stderr and raw stdout
        # We expect:
        # - Debug log containing "stdout_chunk" (raw stdout)
        # - Debug/Error log containing "[RSYNC_ERR]" and "stderr_chunk"

        raw_stdout_logged = False
        stderr_logged = False

        for call_args in mock_logger.debug.call_args_list:
            msg = call_args[0][0]
            if "stdout_chunk" in str(msg):
                raw_stdout_logged = True
            if "[RSYNC_ERR]" in str(msg) and "stderr_chunk" in str(msg):
                stderr_logged = True

        # Also check logger.error just in case
        for call_args in mock_logger.error.call_args_list:
            msg = call_args[0][0]
            if "[RSYNC_ERR]" in str(msg) and "stderr_chunk" in str(msg):
                stderr_logged = True

        self.assertTrue(raw_stdout_logged, "Raw stdout chunk was not logged")
        self.assertTrue(stderr_logged, "Stderr chunk was not logged in real-time")
