import pytest
from unittest.mock import MagicMock, patch
import configparser
import os
import shutil
import pathlib

# Imports from the application
from transfer_manager import transfer_content_rsync
from torrent_mover import _execute_transfer
from system_manager import force_remote_permissions
from transfer_strategies import TransferFile
import process_runner

# Test 1: test_rsync_flags_ignore_metadata
def test_rsync_flags_ignore_metadata(mocker):
    # Mock dependencies
    mock_execute_streaming = mocker.patch('process_runner.execute_streaming_command')
    mock_execute_streaming.return_value = True

    # Mock filesystem operations
    mocker.patch('pathlib.Path.mkdir')
    mocker.patch('os.path.exists', return_value=True)
    mocker.patch('os.path.getsize', return_value=1000)

    # Mock config
    config = MagicMock()
    # Configure __getitem__ to act like a dict
    config_dict = {'host': 'host', 'port': '22', 'username': 'user', 'password': 'pw'}
    config.__getitem__.side_effect = config_dict.get
    config.getint.return_value = 22

    # Mock file tracker
    mock_file_tracker = MagicMock()
    # Ensure it doesn't trigger corruption logic which might complicate flow
    mock_file_tracker.is_corrupted.return_value = False

    # Call transfer_content_rsync
    rsync_options = ["-avh"]
    transfer_content_rsync(
        sftp_config=config,
        remote_path="/remote/path",
        local_path="/local/path",
        torrent_hash="hash",
        rsync_options=rsync_options,
        file_tracker=mock_file_tracker,
        total_size=1000,
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock()
    )

    # Assertions
    # Verify arguments passed to execute_streaming_command
    args, _ = mock_execute_streaming.call_args
    command = args[0]

    # Check for the critical flags
    assert "--no-perms" in command
    assert "--no-owner" in command
    assert "--no-group" in command
    assert "--no-times" in command

# Test 2: test_pre_flight_unlock_called
def test_pre_flight_unlock_called(mocker):
    # Mock dependencies
    mock_force_permissions = mocker.patch('torrent_mover.force_remote_permissions')
    # Mock the actual transfer function so we don't try to run it
    mock_transfer_queue = mocker.patch('torrent_mover.transfer_content_with_queue')

    # Setup inputs
    # Define a file that needs transfer
    files = [TransferFile(source_path="/src/file", dest_path="/dest/file", size=100, torrent_hash="hash")]
    torrent = MagicMock()
    torrent.hash = "hash"

    # Setup Config
    config = configparser.ConfigParser()
    config.add_section('SETTINGS')
    config['SETTINGS']['transfer_mode'] = 'sftp'
    config['SETTINGS']['source_server_section'] = 'SOURCE_SERVER'
    config.add_section('SOURCE_SERVER')
    config.add_section('DESTINATION_SERVER') # Required for unlock trigger

    # Setup SSH Pools
    mock_dest_pool = MagicMock()
    ssh_pools = {
        'SOURCE_SERVER': MagicMock(),
        'DESTINATION_SERVER': mock_dest_pool
    }

    ui = MagicMock()
    file_tracker = MagicMock()

    # Call _execute_transfer
    _execute_transfer(
        transfer_mode='sftp',
        files=files,
        torrent=torrent,
        total_size_calc=100,
        config=config,
        ui=ui,
        file_tracker=file_tracker,
        ssh_connection_pools=ssh_pools,
        dry_run=False,
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock()
    )

    # Assert
    # /dest/file -> dirname is /dest
    # It should call force_remote_permissions on the parent directory of the destination
    mock_force_permissions.assert_called_once_with("/dest", mock_dest_pool)

# Test 3: test_cleanup_handles_directories
def test_cleanup_handles_directories(mocker):
    """
    Simulate the Atomic Fallback path where a corrupted/stalled file is removed.
    We mock os.path.isdir to return True, so it should call shutil.rmtree instead of os.remove.
    """
    # Mock dependencies
    mock_execute_streaming = mocker.patch('process_runner.execute_streaming_command')

    # Mock filesystem operations to prevent PermissionError
    mocker.patch('pathlib.Path.mkdir')

    # Mock file tracker to report corruption, triggering the cleanup logic
    mock_file_tracker = MagicMock()
    mock_file_tracker.is_corrupted.return_value = True

    # Mock OS/Shutil operations
    mock_isdir = mocker.patch('os.path.isdir', return_value=True)
    mock_rmtree = mocker.patch('shutil.rmtree')
    mock_remove = mocker.patch('os.remove')
    mocker.patch('os.path.exists', return_value=True) # File exists
    mocker.patch('os.path.getsize', return_value=100)

    # Mock Config
    config = MagicMock()
    config_dict = {'host': 'h', 'port': '22', 'username': 'u', 'password': 'p'}
    config.__getitem__.side_effect = config_dict.get
    config.getint.return_value = 22

    # We want execute_streaming to succeed eventually so we don't raise error/retry forever
    mock_execute_streaming.return_value = True

    # Needed for verification check at the end of transfer_content_rsync
    # The function does a local import, so we patch it in ssh_manager
    mock_pool_cls = mocker.patch('ssh_manager.SSHConnectionPool')
    mock_pool_instance = mock_pool_cls.return_value
    mock_conn_ctx = MagicMock()
    mock_pool_instance.get_connection.return_value = mock_conn_ctx
    # yield (sftp, ssh)
    mock_conn_ctx.__enter__.return_value = (MagicMock(), MagicMock())

    mocker.patch('ssh_manager.batch_get_remote_sizes', return_value={'/remote/path': 100})

    # Call function
    transfer_content_rsync(
        sftp_config=config,
        remote_path="/remote/path",
        local_path="/local/path",
        torrent_hash="hash",
        rsync_options=[],
        file_tracker=mock_file_tracker,
        total_size=100,
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock()
    )

    # Assertions
    # It should check if it is a directory
    mock_isdir.assert_called_with("/local/path")
    # It should call rmtree because isdir returned True
    mock_rmtree.assert_called_with("/local/path")
    # It should NOT call os.remove
    mock_remove.assert_not_called()
