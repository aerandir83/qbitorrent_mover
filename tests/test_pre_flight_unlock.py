import pytest
from unittest.mock import MagicMock, patch
import configparser
import os
from torrent_mover import _execute_transfer
from transfer_strategies import TransferFile
from ssh_manager import SSHConnectionPool

@pytest.fixture
def mock_config():
    config = configparser.ConfigParser()
    config['SETTINGS'] = {
        'transfer_mode': 'sftp',
        'source_server_section': 'SOURCE_SERVER',
        'sftp_chunk_size_kb': '64',
        'sftp_download_limit_mbps': '0',
        'max_concurrent_downloads': '4',
    }
    config['SOURCE_SERVER'] = {
        'host': 'source_host',
        'username': 'user',
        'password': 'password',
        'port': '22'
    }
    # Default without DESTINATION_SERVER
    return config

@pytest.fixture
def mock_ui():
    ui = MagicMock()
    return ui

@pytest.fixture
def mock_file_tracker():
    return MagicMock()

@pytest.fixture
def mock_ssh_pool():
    return MagicMock(spec=SSHConnectionPool)

@patch('torrent_mover.force_remote_permissions')
@patch('torrent_mover.transfer_content_with_queue') # Mock the actual transfer to avoid errors
def test_unlock_triggered_sftp(mock_transfer, mock_force_perms, mock_config, mock_ui, mock_file_tracker, mock_ssh_pool):
    """
    Verify pre-flight unlock is triggered for SFTP and uses parent directory.
    """
    # Setup
    mock_config['DESTINATION_SERVER'] = {'host': 'dest'}
    mock_config['SETTINGS']['transfer_mode'] = 'sftp'

    ssh_pools = {
        'SOURCE_SERVER': mock_ssh_pool,
        'DESTINATION_SERVER': mock_ssh_pool
    }

    files = [
        TransferFile(source_path="/src/file.mkv", dest_path="/dest/Movie/file.mkv", size=100, torrent_hash="hash")
    ]

    torrent = MagicMock()
    torrent.hash = "hash"
    torrent.name = "Test"

    # Execute
    _execute_transfer(
        transfer_mode='sftp',
        files=files,
        torrent=torrent,
        total_size_calc=100,
        config=mock_config,
        ui=mock_ui,
        file_tracker=mock_file_tracker,
        ssh_connection_pools=ssh_pools,
        dry_run=False,
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock()
    )

    # Assert
    # For SFTP, dest_path is /dest/Movie/file.mkv. We expect unlock on /dest/Movie (dirname).
    # Since os.path.dirname is platform dependent, we should be careful.
    # Assuming Linux environment for tests.
    expected_path = os.path.dirname("/dest/Movie/file.mkv")
    mock_force_perms.assert_called_once_with(expected_path, mock_ssh_pool)

@patch('torrent_mover.force_remote_permissions')
@patch('torrent_mover.transfer_content_rsync') # Mock rsync transfer
def test_unlock_triggered_rsync(mock_transfer, mock_force_perms, mock_config, mock_ui, mock_file_tracker, mock_ssh_pool):
    """
    Verify pre-flight unlock is triggered for Rsync and uses dest_path directly.
    """
    # Setup
    mock_config['DESTINATION_SERVER'] = {'host': 'dest'}
    mock_config['SETTINGS']['transfer_mode'] = 'rsync'

    ssh_pools = {
        'SOURCE_SERVER': mock_ssh_pool,
        'DESTINATION_SERVER': mock_ssh_pool
    }

    # For rsync, dest_path is the root content path
    files = [
        TransferFile(source_path="/src/Movie", dest_path="/dest/Movie", size=100, torrent_hash="hash")
    ]

    torrent = MagicMock()
    torrent.hash = "hash"
    torrent.name = "Test"

    # Execute
    _execute_transfer(
        transfer_mode='rsync',
        files=files,
        torrent=torrent,
        total_size_calc=100,
        config=mock_config,
        ui=mock_ui,
        file_tracker=mock_file_tracker,
        ssh_connection_pools=ssh_pools,
        dry_run=False,
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock()
    )

    # Assert
    # For Rsync, we expect unlock on /dest/Movie directly.
    mock_force_perms.assert_called_once_with("/dest/Movie", mock_ssh_pool)

@patch('torrent_mover.force_remote_permissions')
@patch('torrent_mover.transfer_content_with_queue')
def test_unlock_skipped_dry_run(mock_transfer, mock_force_perms, mock_config, mock_ui, mock_file_tracker, mock_ssh_pool):
    """
    Verify pre-flight unlock is SKIPPED during dry run.
    """
    mock_config['DESTINATION_SERVER'] = {'host': 'dest'}
    ssh_pools = {'SOURCE_SERVER': mock_ssh_pool, 'DESTINATION_SERVER': mock_ssh_pool}
    files = [TransferFile(source_path="/s", dest_path="/d", size=10, torrent_hash="h")]
    torrent = MagicMock()
    torrent.hash = "h"

    _execute_transfer(
        transfer_mode='sftp',
        files=files,
        torrent=torrent,
        total_size_calc=10,
        config=mock_config,
        ui=mock_ui,
        file_tracker=mock_file_tracker,
        ssh_connection_pools=ssh_pools,
        dry_run=True, # <--- DRY RUN
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock()
    )

    mock_force_perms.assert_not_called()

@patch('torrent_mover.force_remote_permissions')
@patch('torrent_mover.transfer_content_with_queue')
def test_unlock_skipped_no_dest_server(mock_transfer, mock_force_perms, mock_config, mock_ui, mock_file_tracker, mock_ssh_pool):
    """
    Verify pre-flight unlock is SKIPPED if DESTINATION_SERVER not in config.
    """
    # Ensure no DESTINATION_SERVER in config
    if 'DESTINATION_SERVER' in mock_config:
        del mock_config['DESTINATION_SERVER']

    ssh_pools = {'SOURCE_SERVER': mock_ssh_pool}
    files = [TransferFile(source_path="/s", dest_path="/d", size=10, torrent_hash="h")]
    torrent = MagicMock()
    torrent.hash = "h"

    _execute_transfer(
        transfer_mode='sftp',
        files=files,
        torrent=torrent,
        total_size_calc=10,
        config=mock_config,
        ui=mock_ui,
        file_tracker=mock_file_tracker,
        ssh_connection_pools=ssh_pools,
        dry_run=False,
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock()
    )

    mock_force_perms.assert_not_called()
