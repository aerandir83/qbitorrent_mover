import pytest
from unittest.mock import MagicMock, patch, call, ANY
import configparser
from pathlib import Path
import os
from concurrent.futures import Future

from transfer_manager import (
    transfer_content_sftp,
    transfer_content_sftp_upload,
    FileTransferTracker
)
from transfer_strategies import TransferFile
from ssh_manager import SSHConnectionPool
from ui import UIManagerV2

@pytest.fixture
def mock_sftp_pool():
    pool = MagicMock(spec=SSHConnectionPool)
    pool.host = "test.host"
    pool.port = 22
    
    # Mock sftp connection context manager
    mock_conn = MagicMock()
    mock_sftp = MagicMock()
    mock_ssh = MagicMock()
    mock_conn.__enter__.return_value = (mock_sftp, mock_ssh)
    pool.get_connection.return_value = mock_conn
    return pool, mock_sftp

@pytest.fixture
def mock_ui():
    ui = MagicMock(spec=UIManagerV2)
    ui._lock = MagicMock()
    ui._lock.__enter__ = MagicMock()
    ui._lock.__exit__ = MagicMock()
    ui._file_progress = {}
    return ui

@pytest.fixture
def mock_file_tracker():
    tracker = MagicMock(spec=FileTransferTracker)
    tracker.is_corrupted.return_value = False
    tracker.get_cache_location.return_value = None
    tracker.verify_file_integrity.return_value = True
    return tracker

class TestSFTPModes:

    @patch('transfer_manager.SFTPDownloader')
    def test_sftp_download_mode(self, mock_downloader_cls, mock_sftp_pool, mock_ui, mock_file_tracker):
        """Test the 'sftp' mode (download via queue)."""
        pool, _ = mock_sftp_pool
        files = [
            TransferFile(source_path="/remote/file1", dest_path="/local/file1", size=100, torrent_hash="hash"),
            TransferFile(source_path="/remote/file2", dest_path="/local/file2", size=200, torrent_hash="hash")
        ]
        
        # Mock the SFTPDownloader instance
        mock_downloader = mock_downloader_cls.return_value
        mock_downloader.download_file.return_value = None

        log_transfer = MagicMock()
        update_progress = MagicMock()
        
        transfer_content_sftp(
            pool=pool,
            files=files,
            torrent_hash="hash",
            total_size=300,
            log_transfer=log_transfer,
            _update_transfer_progress=update_progress,
            ui=mock_ui,
            file_tracker=mock_file_tracker,
            max_concurrent_downloads=2
        )
        
        # Verify that SFTPDownloader was instantiated
        assert mock_downloader_cls.call_count >= 1 # Could be once per file or once total depending on implementation, actually once per worker usually
        
        # Verify download_file calls
        # Since it uses threads, order is not guaranteed.
        # But we expect 2 calls total.
        assert mock_downloader.download_file.call_count == 2
        
        args_list = [call.kwargs['remote_path'] for call in mock_downloader.download_file.call_args_list]
        assert "/remote/file1" in args_list
        assert "/remote/file2" in args_list

    @patch('transfer_manager._sftp_upload_file')
    def test_sftp_upload_direct_mode(self, mock_upload_file, mock_sftp_pool, mock_ui, mock_file_tracker):
        """Test 'sftp_upload' mode with direct streaming (small files)."""
        source_pool, _ = mock_sftp_pool
        dest_pool = MagicMock(spec=SSHConnectionPool)
        
        files = [("/remote/src/file1", "/remote/dest/file1")]
        total_size = 500 * 1024 * 1024 # 500 MB, under 1GB limit
        
        transfer_content_sftp_upload(
            source_pool=source_pool,
            dest_pool=dest_pool,
            all_files=files,
            torrent_hash="hash",
            ui=mock_ui,
            file_tracker=mock_file_tracker,
            max_concurrent_downloads=2,
            max_concurrent_uploads=2,
            total_size=total_size,
            local_cache_sftp_upload=False
        )
        
        mock_upload_file.assert_called_once()
        args = mock_upload_file.call_args
        assert args[0][2] == "/remote/src/file1" # source
        assert args[0][3] == "/remote/dest/file1" # dest

    @patch('transfer_manager._sftp_download_to_cache')
    @patch('transfer_manager._sftp_upload_from_cache')
    @patch('shutil.rmtree') # prevent actual deletion of temp dir
    def test_sftp_upload_cached_mode(self, mock_rmtree, mock_upload_cache, mock_download_cache, mock_sftp_pool, mock_ui, mock_file_tracker, fs):
        """Test 'sftp_upload' mode with local caching (large files)."""
        source_pool, _ = mock_sftp_pool
        dest_pool = MagicMock(spec=SSHConnectionPool)
        
        # Setup fake fs for cache dir checks if needed
        fs.create_dir("/tmp")
        
        files = [("/remote/src/large_file", "/remote/dest/large_file")]
        total_size = 2 * 1024 * 1024 * 1024 # 2 GB, over 1GB limit
        
        # Mock side effect to create the file in fs when download "happens"
        def download_side_effect(*args, **kwargs):
            # args: source_pool, source_f, local_path, ...
            # local_path is the 3rd arg (index 2)
            local_path = args[2]
            fs.create_file(local_path, contents="fake content")
            return None

        mock_download_cache.side_effect = download_side_effect
        mock_upload_cache.return_value = None

        transfer_content_sftp_upload(
            source_pool=source_pool,
            dest_pool=dest_pool,
            all_files=files,
            torrent_hash="hash",
            ui=mock_ui,
            file_tracker=mock_file_tracker,
            max_concurrent_downloads=2,
            max_concurrent_uploads=2,
            total_size=total_size,
            local_cache_sftp_upload=False # Should be overridden to True due to size
        )
        
        mock_download_cache.assert_called_once()
        mock_upload_cache.assert_called_once()
