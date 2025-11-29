import unittest
from unittest.mock import MagicMock, patch
import tempfile
import shutil
import os
import transfer_manager
from transfer_manager import transfer_content_rsync, _transfer_content_rsync_upload_from_cache, transfer_content_rsync_upload

class TestTransferIntegrity(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.mock_config = {
            'host': 'localhost',
            'port': '22',
            'username': 'user',
            'password': 'password'
        }
        self.mock_config_proxy = MagicMock()
        self.mock_config_proxy.__getitem__.side_effect = self.mock_config.__getitem__
        self.mock_config_proxy.getint.side_effect = lambda k, d=None: int(self.mock_config.get(k, d)) if(self.mock_config.get(k) is not None) else d
        self.mock_config_proxy.get.side_effect = self.mock_config.get

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    @patch('transfer_manager.process_runner.execute_streaming_command')
    @patch('transfer_manager.SpeedMonitor')
    @patch('transfer_manager.SFTPSizeFetcher')
    def test_transfer_content_rsync_no_checksum_by_default(self, mock_fetcher, mock_monitor, mock_execute):
        # New behavior: checksum is NOT added automatically
        mock_execute.return_value = True
        local_path = os.path.join(self.test_dir, 'file.txt')

        try:
            transfer_content_rsync(
                sftp_config=self.mock_config_proxy,
                remote_path='/remote/path',
                local_path=local_path,
                torrent_hash='hash',
                rsync_options=['-av'],
                file_tracker=MagicMock(),
                total_size=1000,
                log_transfer=MagicMock(),
                _update_transfer_progress=MagicMock()
            )
        except Exception:
            pass

        args, _ = mock_execute.call_args
        cmd = args[0]
        self.assertNotIn('--checksum', cmd)

    @patch('transfer_manager.process_runner.execute_streaming_command')
    @patch('transfer_manager.SpeedMonitor')
    @patch('transfer_manager.SFTPSizeFetcher')
    def test_transfer_content_rsync_forces_checksum_with_flag(self, mock_fetcher, mock_monitor, mock_execute):
        # New behavior: checksum is added if flag is True
        mock_execute.return_value = True
        local_path = os.path.join(self.test_dir, 'file.txt')

        try:
            transfer_content_rsync(
                sftp_config=self.mock_config_proxy,
                remote_path='/remote/path',
                local_path=local_path,
                torrent_hash='hash',
                rsync_options=['-av'],
                file_tracker=MagicMock(),
                total_size=1000,
                log_transfer=MagicMock(),
                _update_transfer_progress=MagicMock(),
                force_integrity_check=True
            )
        except Exception:
            pass

        args, _ = mock_execute.call_args
        cmd = args[0]
        self.assertIn('--checksum', cmd)

    @patch('transfer_manager.process_runner.execute_streaming_command')
    @patch('transfer_manager.SpeedMonitor')
    @patch('transfer_manager.SFTPSizeFetcher')
    def test_transfer_content_rsync_keeps_checksum_if_in_config(self, mock_fetcher, mock_monitor, mock_execute):
        # Even if flag is False, if it's in config, it stays
        mock_execute.return_value = True
        local_path = os.path.join(self.test_dir, 'file.txt')

        try:
            transfer_content_rsync(
                sftp_config=self.mock_config_proxy,
                remote_path='/remote/path',
                local_path=local_path,
                torrent_hash='hash',
                rsync_options=['-av', '--checksum'],
                file_tracker=MagicMock(),
                total_size=1000,
                log_transfer=MagicMock(),
                _update_transfer_progress=MagicMock(),
                force_integrity_check=False
            )
        except Exception:
            pass

        args, _ = mock_execute.call_args
        cmd = args[0]
        self.assertIn('--checksum', cmd)

    @patch('transfer_manager.process_runner.execute_streaming_command')
    @patch('transfer_manager.SpeedMonitor')
    @patch('transfer_manager.SFTPSizeFetcher')
    def test_transfer_content_rsync_upload_from_cache_forces_checksum_with_flag(self, mock_fetcher, mock_monitor, mock_execute):
        # Check upload from cache respects the flag
        mock_execute.return_value = True
        local_path = os.path.join(self.test_dir, 'file.txt')
        with open(local_path, 'w') as f:
            f.write('test')

        try:
            _transfer_content_rsync_upload_from_cache(
                dest_config=self.mock_config_proxy,
                local_path=local_path,
                remote_path='/remote/path',
                torrent_hash='hash',
                rsync_options=['-av'],
                total_size=1000,
                log_transfer=MagicMock(),
                _update_transfer_progress=MagicMock(),
                force_integrity_check=True
            )
        except Exception:
            pass

        args, _ = mock_execute.call_args
        cmd = args[0]
        self.assertIn('--checksum', cmd)

    @patch('transfer_manager.transfer_content_rsync')
    @patch('transfer_manager._transfer_content_rsync_upload_from_cache')
    def test_transfer_content_rsync_upload_propagates_flag(self, mock_upload, mock_download):
        # Verify propagation
        mock_download.return_value = None
        mock_upload.return_value = None

        source_config = self.mock_config_proxy
        dest_config = self.mock_config_proxy

        transfer_content_rsync_upload(
            source_config=source_config,
            dest_config=dest_config,
            rsync_options=['-av'],
            source_content_path='/source/file',
            dest_content_path='/dest/file',
            torrent_hash='hash',
            file_tracker=MagicMock(),
            total_size=1000,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock(),
            dry_run=False,
            is_folder=False,
            force_integrity_check=True
        )

        # Check download call
        kwargs_dl = mock_download.call_args[1]
        self.assertTrue(kwargs_dl['force_integrity_check'])

        # Check upload call
        kwargs_ul = mock_upload.call_args[1]
        self.assertTrue(kwargs_ul['force_integrity_check'])
