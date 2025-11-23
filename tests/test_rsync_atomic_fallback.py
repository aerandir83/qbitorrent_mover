import pytest
from unittest.mock import MagicMock, patch
import configparser
import os
import shutil
from transfer_manager import transfer_content_rsync
from tests.mocks.mock_ssh import MockSSHConnectionPool

@pytest.fixture
def rsync_config():
    config = configparser.ConfigParser()
    config['SOURCE_SERVER'] = {
        'host': 'source.server.com',
        'port': '2222',
        'username': 'user',
        'password': 'pass'
    }
    return config

@patch('process_runner.execute_streaming_command')
@patch('transfer_manager.SSHConnectionPool')
@patch('ssh_manager.batch_get_remote_sizes')
def test_atomic_fallback_stalled_file(
    mock_batch_get_sizes,
    MockSshPool,
    mock_execute_command,
    rsync_config,
    fs
):
    """
    Verifies that a stalled file (small size) is removed before retry on the 2nd attempt.
    """
    local_path = "/local/stalled_file"
    fs.create_dir("/local")

    # Create a small file (stalled)
    fs.create_file(local_path, contents="stalled") # < 1024 bytes

    mock_file_tracker = MagicMock()
    mock_file_tracker.is_corrupted.return_value = False

    # This list tracks if the file existed when execute_command was called
    file_existence_log = []

    # Mock execute_command to fail on 1st attempt, succeed on 2nd
    def side_effect(*args, **kwargs):
        # Record if file exists at the moment rsync is called
        file_existence_log.append(os.path.exists(local_path))

        if mock_execute_command.call_count == 1:
            # Attempt 1: The stalled file should still exist (check is > 1)
            return False
        else:
            # Attempt 2: The stalled file should have been DELETED by fallback logic
            # Simulate rsync succeeding and creating the full file
            fs.create_file(local_path, contents="a" * 2000) # Full size
            return True

    mock_execute_command.side_effect = side_effect

    mock_batch_get_sizes.return_value = {"/remote/file": 2000}
    MockSshPool.return_value = MockSSHConnectionPool(host="", port=22, username="", password="")

    transfer_content_rsync(
        sftp_config=rsync_config['SOURCE_SERVER'],
        remote_path="/remote/file",
        local_path=local_path,
        torrent_hash="hash",
        rsync_options=[],
        file_tracker=mock_file_tracker,
        total_size=2000,
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock(),
        dry_run=False
    )

    assert mock_execute_command.call_count == 2

    # Verification:
    # Call 1: File existed (True) - Logic didn't trigger because attempt=1
    # Call 2: File did NOT exist (False) - Logic triggered because attempt=2 and size < 1024
    assert file_existence_log == [True, False]

@patch('process_runner.execute_streaming_command')
@patch('transfer_manager.SSHConnectionPool')
@patch('ssh_manager.batch_get_remote_sizes')
def test_atomic_fallback_corruption_flag(
    mock_batch_get_sizes,
    MockSshPool,
    mock_execute_command,
    rsync_config,
    fs
):
    """
    Verifies that if is_corrupted is True, the file is removed immediately (even on attempt 1).
    """
    local_path = "/local/corrupt_file"
    fs.create_dir("/local")
    fs.create_file(local_path, contents="corrupt data")

    mock_file_tracker = MagicMock()
    mock_file_tracker.is_corrupted.return_value = True

    file_existence_log = []

    def side_effect(*args, **kwargs):
        file_existence_log.append(os.path.exists(local_path))
        fs.create_file(local_path, contents="a" * 2000)
        return True

    mock_execute_command.side_effect = side_effect

    mock_batch_get_sizes.return_value = {"/remote/file": 2000}
    MockSshPool.return_value = MockSSHConnectionPool(host="", port=22, username="", password="")

    transfer_content_rsync(
        sftp_config=rsync_config['SOURCE_SERVER'],
        remote_path="/remote/file",
        local_path=local_path,
        torrent_hash="hash",
        rsync_options=[],
        file_tracker=mock_file_tracker,
        total_size=2000,
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock(),
        dry_run=False
    )

    # Logic triggers on attempt 1 because is_corrupted is True
    assert file_existence_log == [False]

@patch('process_runner.execute_streaming_command')
@patch('transfer_manager.SSHConnectionPool')
@patch('ssh_manager.batch_get_remote_sizes')
def test_atomic_fallback_directory_cleanup(
    mock_batch_get_sizes,
    MockSshPool,
    mock_execute_command,
    rsync_config,
    fs
):
    """
    Verifies that if the local path is a directory, it is removed using shutil.rmtree.
    """
    local_path = "/local/corrupt_dir"
    fs.create_dir(local_path)
    # create some files inside
    fs.create_file(os.path.join(local_path, "somefile"), contents="data")

    mock_file_tracker = MagicMock()
    mock_file_tracker.is_corrupted.return_value = True # Force fallback on 1st try

    def side_effect(*args, **kwargs):
        # The directory should have been removed by now by the fallback logic
        if os.path.exists(local_path):
             # If it exists, it must not be a directory if we are to create a file
             # But actually we expect it to be GONE.
             raise Exception(f"Cleanup failed: {local_path} still exists")

        # rsync succeeds
        fs.create_file(local_path, contents="a" * 2000)
        return True

    mock_execute_command.side_effect = side_effect
    mock_batch_get_sizes.return_value = {"/remote/file": 2000}
    MockSshPool.return_value = MockSSHConnectionPool(host="", port=22, username="", password="")

    transfer_content_rsync(
        sftp_config=rsync_config['SOURCE_SERVER'],
        remote_path="/remote/file",
        local_path=local_path,
        torrent_hash="hash",
        rsync_options=[],
        file_tracker=mock_file_tracker,
        total_size=2000,
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock(),
        dry_run=False
    )
