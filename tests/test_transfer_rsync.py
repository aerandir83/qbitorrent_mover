import pytest
from unittest.mock import MagicMock, patch
import configparser
from pathlib import Path

# Function to test
from transfer_manager import transfer_content_rsync

# Mocks
from tests.mocks.mock_ssh import MockSSHConnectionPool

@pytest.fixture
def rsync_config():
    """Provides config for rsync."""
    config = configparser.ConfigParser()
    config['SOURCE_SERVER'] = {
        'host': 'source.server.com',
        'port': '22',
        'username': 'user',
        'password': 'pass'
    }
    return config

@pytest.fixture
def mock_file_tracker():
    """Mock FileTransferTracker."""
    tracker = MagicMock()
    tracker.is_corrupted.return_value = False
    return tracker

@patch('process_runner.execute_streaming_command')
@patch('ssh_manager.SSHConnectionPool')
@patch('ssh_manager.batch_get_remote_sizes')
def test_rsync_delta_transfer_logic(
    mock_batch_get_sizes,
    MockSshPool, # Note: Patched class
    mock_execute_command,
    rsync_config,
    mock_file_tracker,
    fs # This is the pyfakefs fixture
):
    """
    Tests the "Delta Transfer" logic from Directive #4.
    Verifies that transfer_content_rsync (our refactored function)
    calls the process_runner with the correct, checksum-enabled command.
    """
    # --- Setup ---
    # 1. Create virtual filesystem
    source_path = "/remote/data/My.Movie"
    local_path = "/local/downloads/My.Movie"
    local_parent = "/local/downloads"

    # We only need the *local* filesystem, as rsync's source is remote.
    # pyfakefs will create the local parent directory for us.
    fs.create_dir(local_parent)

    # Create an "incomplete" file locally to simulate a previous failed transfer
    fs.create_file(local_path, contents="incomplete data")

    # Define a side effect for the mock execute command to simulate a successful transfer
    def rsync_side_effect(*args, **kwargs):
        """Simulates rsync writing the file to the fake filesystem."""
        if fs.exists(local_path):
            fs.remove(local_path)
        fs.create_file(local_path, contents="a" * 1000)  # 1000 bytes, matching total_size
        return True

    mock_execute_command.side_effect = rsync_side_effect

    # 2. Setup Mocks
    mock_sftp_config = rsync_config['SOURCE_SERVER']
    mock_callbacks = (MagicMock(), MagicMock()) # log_transfer, _update_transfer_progress
    rsync_options = ["-avh", "--partial"] # User's base options
    total_size = 1000

    # Mock the remote size check to return a size
    mock_batch_get_sizes.return_value = {source_path: total_size}

    # Configure the mock SSH pool
    mock_pool_instance = MockSSHConnectionPool(host="", port=22, username="", password="")
    MockSshPool.return_value = mock_pool_instance

    # --- Execute ---
    transfer_content_rsync(
        sftp_config=mock_sftp_config,
        remote_path=source_path,
        local_path=local_path,
        torrent_hash="hash123",
        rsync_options=rsync_options,
        file_tracker=mock_file_tracker,
        total_size=total_size,
        log_transfer=mock_callbacks[0],
        _update_transfer_progress=mock_callbacks[1],
        dry_run=False
    )

    # --- Assert ---
    # 1. Assert our process runner was called
    assert mock_execute_command.call_count == 1

    # 2. Get the command that was passed to the runner
    call_args = mock_execute_command.call_args[0]
    called_command = call_args[0] # The 'command' list

    # 3. Verify the command (Directive 4 - Delta Transfer & Integrity)

    # Assert it includes the CHECKSUM flag (for integrity)
    assert "--checksum" in called_command or "-c" in called_command

    # Assert it includes the progress flag (for our process runner)
    assert "--info=progress2" in called_command

    # Assert it includes the user's options
    assert "-avh" in called_command
    assert "--partial" in called_command

    # Assert it has the correct source and destination
    assert "user@source.server.com:/remote/data/My.Movie" in called_command
    assert "/local/downloads" in called_command

    # Assert it's an rsync command
    assert "rsync" in called_command
