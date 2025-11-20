import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
import shutil
from transfer_manager import _sftp_download_to_cache
from collections import namedtuple

Usage = namedtuple('Usage', ['total', 'used', 'free'])

class TestCapacityCheck:

    @patch('transfer_manager.shutil.disk_usage')
    @patch('transfer_manager.time.sleep') # Mock sleep to speed up retries
    def test_insufficient_space(self, mock_sleep, mock_disk_usage):
        # Setup mocks
        source_pool = MagicMock()
        sftp_client = MagicMock()
        ssh_client = MagicMock()

        # Setup sftp.stat
        remote_stat = MagicMock()
        remote_stat.st_size = 1024 * 1024 * 100 # 100 MB
        sftp_client.stat.return_value = remote_stat

        # Setup context manager for source_pool.get_connection()
        source_pool.get_connection.return_value.__enter__.return_value = (sftp_client, ssh_client)

        # Setup local_cache_path
        local_cache_path = MagicMock(spec=Path)
        local_cache_path.parent = Path("/tmp")
        local_cache_path.exists.return_value = False
        local_cache_path.name = "file.mkv"

        # Setup disk_usage to return less space
        # free space = 50 MB
        mock_disk_usage.return_value = Usage(1000, 500, 1024 * 1024 * 50)

        ui = MagicMock()
        file_tracker = MagicMock()

        # Expect exception
        # _sftp_download_to_cache is decorated with retry, so it will retry and then fail
        with pytest.raises(Exception) as excinfo:
             _sftp_download_to_cache(source_pool, "/remote/file.mkv", local_cache_path, "hash", ui, file_tracker)

        assert "Insufficient local cache space" in str(excinfo.value)

    @patch('transfer_manager.shutil.disk_usage')
    def test_sufficient_space(self, mock_disk_usage):
        # Setup mocks
        source_pool = MagicMock()
        sftp_client = MagicMock()
        ssh_client = MagicMock()

        # Setup sftp.stat
        remote_stat = MagicMock()
        remote_stat.st_size = 1024 * 1024 * 100 # 100 MB
        sftp_client.stat.return_value = remote_stat

        # Setup context manager for source_pool.get_connection()
        source_pool.get_connection.return_value.__enter__.return_value = (sftp_client, ssh_client)

        # Setup local_cache_path
        local_cache_path = MagicMock(spec=Path)
        local_cache_path.parent = Path("/tmp")
        local_cache_path.exists.return_value = False
        local_cache_path.name = "file.mkv"

        # Setup disk_usage to return enough space
        # free space = 200 MB
        mock_disk_usage.return_value = Usage(1000, 500, 1024 * 1024 * 200)

        ui = MagicMock()
        file_tracker = MagicMock()

        # Mock sftp open/read so it doesn't fail later
        remote_f = MagicMock()
        remote_f.read.return_value = b"" # EOF
        sftp_client.open.return_value.__enter__.return_value = remote_f

        # Mock open()
        with patch("builtins.open", MagicMock()):
            # Should not raise
            _sftp_download_to_cache(source_pool, "/remote/file.mkv", local_cache_path, "hash", ui, file_tracker)
