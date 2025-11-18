import pytest
from unittest.mock import MagicMock, patch, call
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
        'port': '2222',
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

    # Configure the mock SSH pool for verification step
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

# New tests below

class TestRsyncCommandConstruction:
    @patch('process_runner.execute_streaming_command')
    @patch('transfer_manager.SSHConnectionPool')
    @patch('ssh_manager.batch_get_remote_sizes')
    def test_rsync_command_structure_and_escaping(
        self,
        mock_batch_get_sizes,
        MockSshPool,
        mock_execute_command,
        rsync_config,
        mock_file_tracker,
        fs # Add pyfakefs fixture
    ):
        """
        Verifies the core structure of the rsync command, including path quoting.
        """
        # --- Setup ---
        source_path = "/remote/data/A Movie (2023) With Spaces"
        local_path = "/local/downloads/A Movie (2023) With Spaces"
        local_parent = "/local/downloads"

        fs.create_dir(local_parent) # Create directory in fake filesystem

        mock_sftp_config = rsync_config['SOURCE_SERVER']
        mock_callbacks = (MagicMock(), MagicMock())
        rsync_options = ["--partial"]
        total_size = 1000

        def rsync_side_effect(*args, **kwargs):
            fs.create_file(local_path, contents="a" * total_size)
            return True

        mock_execute_command.side_effect = rsync_side_effect
        mock_batch_get_sizes.return_value = {source_path: total_size}

        # Configure the mock SSH pool for verification step
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
        mock_execute_command.assert_called_once()
        called_command = mock_execute_command.call_args[0][0]

        # 1. Assert essential flags
        assert "rsync" in called_command
        assert "--checksum" in called_command
        assert "--info=progress2" in called_command
        assert "--partial" in called_command # from user options

        # 2. Assert source and destination paths are present and correctly formatted
        expected_remote_spec = f"user@source.server.com:{source_path}"
        assert expected_remote_spec in called_command
        assert local_parent in called_command

        # 3. Assert sshpass is used correctly
        assert "sshpass" in called_command
        assert "-p" in called_command
        assert "pass" in called_command

    @patch('process_runner.execute_streaming_command', return_value=True)
    @patch('transfer_manager.SSHConnectionPool') # Patch the pool where it's used for verification
    @patch('ssh_manager.batch_get_remote_sizes')
    @patch('transfer_manager._get_ssh_command') # <-- CORRECTED PATCH TARGET
    def test_rsync_ssh_command_construction(
        self,
        mock_get_ssh_cmd,
        mock_batch_get_sizes,
        MockSshPool,
        mock_execute_command,
        rsync_config,
        mock_file_tracker,
        fs # Add pyfakefs fixture
    ):
        """
        Verifies that the -e option correctly uses the ssh_manager._get_ssh_command.
        """
        # --- Setup ---
        fs.create_dir("/local") # Create directory in fake filesystem
        mock_get_ssh_cmd.return_value = "ssh -p 2222 -o CustomOption"
        mock_sftp_config = rsync_config['SOURCE_SERVER']
        mock_callbacks = (MagicMock(), MagicMock())
        mock_batch_get_sizes.return_value = {"/remote/file": 100}

        # Configure the mock SSH pool for verification step
        mock_pool_instance = MockSSHConnectionPool(host="", port=22, username="", password="")
        MockSshPool.return_value = mock_pool_instance

        def rsync_side_effect(*args, **kwargs):
            fs.create_file("/local/file", contents="a" * 100)
            return True
        mock_execute_command.side_effect = rsync_side_effect

        # --- Execute ---
        transfer_content_rsync(
            sftp_config=mock_sftp_config,
            remote_path="/remote/file",
            local_path="/local/file",
            torrent_hash="hash123",
            rsync_options=[],
            file_tracker=mock_file_tracker,
            total_size=100,
            log_transfer=mock_callbacks[0],
            _update_transfer_progress=mock_callbacks[1],
            dry_run=False
        )

        # --- Assert ---
        mock_execute_command.assert_called_once()
        called_command = mock_execute_command.call_args[0][0]

        # 1. Assert that the -e flag is present
        assert "-e" in called_command

        # 2. Assert that our mocked ssh command is the value for -e
        e_index = called_command.index("-e")
        assert called_command[e_index + 1] == "ssh -p 2222 -o CustomOption"

        # 3. Assert that _get_ssh_command was called with the correct port
        mock_get_ssh_cmd.assert_called_once_with(2222)

    @patch('process_runner.execute_streaming_command', return_value=True)
    @patch('ssh_manager.SSHConnectionPool')
    @patch('ssh_manager.batch_get_remote_sizes')
    def test_rsync_dry_run_logic(
        self,
        mock_batch_get_sizes,
        MockSshPool,
        mock_execute_command,
        rsync_config,
        mock_file_tracker,
        fs # Add pyfakefs fixture
    ):
        """
        Verifies that in dry_run mode, the rsync command is not executed.
        """
        # --- Setup ---
        fs.create_dir("/local") # Create directory in fake filesystem
        mock_sftp_config = rsync_config['SOURCE_SERVER']
        mock_callbacks = (MagicMock(), MagicMock())
        update_progress_callback = mock_callbacks[1]
        mock_batch_get_sizes.return_value = {"/remote/file": 1024}

        # --- Execute ---
        transfer_content_rsync(
            sftp_config=mock_sftp_config,
            remote_path="/remote/file",
            local_path="/local/file",
            torrent_hash="hash123",
            rsync_options=[],
            file_tracker=mock_file_tracker,
            total_size=1024,
            log_transfer=mock_callbacks[0],
            _update_transfer_progress=update_progress_callback,
            dry_run=True
        )

        # --- Assert ---
        # 1. Main assertion: the command runner is NOT called
        mock_execute_command.assert_not_called()

        # 2. Assert that the progress callback was updated to 100%
        update_progress_callback.assert_called_with("hash123", 1.0, 1024, 1024)