import unittest
from unittest.mock import MagicMock, patch, call
import configparser
from pathlib import Path
import os
import shutil

# Function to test
from transfer_manager import (
    transfer_content_rsync,
    transfer_content_rsync_upload,
    _transfer_content_rsync_upload_from_cache
)

class TestTransferManagerHeartbeat(unittest.TestCase):

    def setUp(self):
        self.config = configparser.ConfigParser()
        self.config['SOURCE_SERVER'] = {
            'host': 'source.server.com',
            'port': '2222',
            'username': 'user',
            'password': 'pass'
        }
        self.config['DESTINATION_SERVER'] = {
            'host': 'dest.server.com',
            'port': '22',
            'username': 'dest_user',
            'password': 'dest_pass'
        }
        self.config['SETTINGS'] = {
             'local_cache_path': '/tmp/cache'
        }
        self.file_tracker = MagicMock()
        self.file_tracker.is_corrupted.return_value = False

        self.log_transfer = MagicMock()
        self.update_progress = MagicMock()
        self.heartbeat = MagicMock()

    @patch('process_runner.execute_streaming_command', return_value=True)
    @patch('transfer_manager.SSHConnectionPool')
    @patch('ssh_manager.batch_get_remote_sizes')
    @patch('transfer_manager._get_ssh_command')
    @patch('os.path.exists')
    @patch('os.path.getsize')
    @patch('pathlib.Path.mkdir')
    @patch('ssh_manager.SSHConnectionPool') # For verification inside transfer_content_rsync
    def test_transfer_content_rsync_passes_heartbeat(
        self,
        MockSshPoolVerification,
        mock_mkdir,
        mock_getsize,
        mock_exists,
        mock_get_ssh,
        mock_batch_sizes,
        MockSshPool,
        mock_execute
    ):
        # Setup for verification phase
        mock_exists.return_value = True
        mock_getsize.return_value = 1000
        mock_batch_sizes.return_value = {"/remote/path": 1000}
        mock_get_ssh.return_value = "-o Option"

        transfer_content_rsync(
            sftp_config=self.config['SOURCE_SERVER'],
            remote_path="/remote/path",
            local_path="/local/path",
            torrent_hash="hash",
            rsync_options=[],
            file_tracker=self.file_tracker,
            total_size=1000,
            log_transfer=self.log_transfer,
            _update_transfer_progress=self.update_progress,
            heartbeat_callback=self.heartbeat
        )

        # Check if execute_streaming_command was called with heartbeat_callback
        mock_execute.assert_called()
        call_kwargs = mock_execute.call_args.kwargs
        self.assertIn('heartbeat_callback', call_kwargs)
        self.assertEqual(call_kwargs['heartbeat_callback'], self.heartbeat)

    @patch('process_runner.execute_streaming_command', return_value=True)
    def test_transfer_content_rsync_upload_from_cache_passes_heartbeat(self, mock_execute):
        _transfer_content_rsync_upload_from_cache(
            dest_config=self.config['DESTINATION_SERVER'],
            local_path="/local/cache/file",
            remote_path="/remote/dest/file",
            torrent_hash="hash",
            rsync_options=[],
            total_size=1000,
            log_transfer=self.log_transfer,
            _update_transfer_progress=self.update_progress,
            heartbeat_callback=self.heartbeat
        )

        mock_execute.assert_called()
        call_kwargs = mock_execute.call_args.kwargs
        self.assertIn('heartbeat_callback', call_kwargs)
        self.assertEqual(call_kwargs['heartbeat_callback'], self.heartbeat)

    @patch('transfer_manager._transfer_content_rsync_upload_from_cache')
    @patch('transfer_manager.transfer_content_rsync')
    @patch('shutil.rmtree')
    @patch('pathlib.Path.mkdir')
    @patch('shutil.disk_usage')
    def test_transfer_content_rsync_upload_passes_heartbeat_to_subcalls(
        self,
        mock_disk_usage,
        mock_mkdir,
        mock_rmtree,
        mock_transfer_rsync,
        mock_upload_from_cache
    ):
        # Mock disk usage to avoid NoSpace error
        mock_usage = MagicMock()
        mock_usage.free = 1024**4 # Lots of space
        mock_disk_usage.return_value = mock_usage

        # This function calls the other two. We just check it passes the callback.
        transfer_content_rsync_upload(
            source_config=self.config['SOURCE_SERVER'],
            dest_config=self.config['DESTINATION_SERVER'],
            rsync_options=[],
            source_content_path="/source/file",
            dest_content_path="/dest/file",
            torrent_hash="hash",
            file_tracker=self.file_tracker,
            total_size=1000,
            log_transfer=self.log_transfer,
            _update_transfer_progress=self.update_progress,
            dry_run=False,
            is_folder=False,
            heartbeat_callback=self.heartbeat
        )

        # Check download call
        mock_transfer_rsync.assert_called()
        call_kwargs_dl = mock_transfer_rsync.call_args.kwargs
        self.assertEqual(call_kwargs_dl['heartbeat_callback'], self.heartbeat)

        # Check upload call
        mock_upload_from_cache.assert_called()
        call_kwargs_ul = mock_upload_from_cache.call_args.kwargs
        self.assertEqual(call_kwargs_ul['heartbeat_callback'], self.heartbeat)

if __name__ == '__main__':
    unittest.main()
