import pytest
from unittest.mock import MagicMock, patch, call
import configparser
from pathlib import Path

# Function to test
from transfer_manager import (
    transfer_content_rsync,
    transfer_content_rsync_upload,
    _transfer_content_rsync_upload_from_cache
)
from utils import RemoteTransferError

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

    # Assert data-only flags (Directive: No Metadata)
    assert "--no-times" in called_command
    assert "--no-perms" in called_command
    assert "--no-owner" in called_command
    assert "--no-group" in called_command

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

        # Assert --timeout is NOT present (it's now adaptive)
        assert "--timeout" not in " ".join(called_command)

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


class TestRsyncResilience:
    """Tests resilience features like retries and error handling."""

    @patch('transfer_manager.SSHConnectionPool')
    @patch('ssh_manager.batch_get_remote_sizes')
    @patch('process_runner.execute_streaming_command')
    def test_rsync_success_on_first_try(
        self,
        mock_execute_command,
        mock_batch_get_sizes,
        MockSshPool,
        rsync_config,
        mock_file_tracker,
        fs
    ):
        """
        Tests that the function succeeds on the first try when execute returns True.
        """
        # --- Setup ---
        source_path = "/remote/success.file"
        local_path = "/local/success.file"
        fs.create_dir("/local")

        # Mock a successful execution that creates the file
        def rsync_side_effect(*args, **kwargs):
            fs.create_file(local_path, contents="a" * 1000)
            return True
        mock_execute_command.side_effect = rsync_side_effect

        # Mock the post-transfer verification
        mock_batch_get_sizes.return_value = {source_path: 1000}
        MockSshPool.return_value = MockSSHConnectionPool(host="", port=22, username="", password="")

        # --- Execute ---
        transfer_content_rsync(
            sftp_config=rsync_config['SOURCE_SERVER'],
            remote_path=source_path,
            local_path=local_path,
            torrent_hash="hash_success",
            rsync_options=[],
            file_tracker=mock_file_tracker,
            total_size=1000,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock(),
            dry_run=False,
            rsync_timeout=60 # Explicitly set to 60 to match test expectations
        )

        # --- Assert ---
        mock_execute_command.assert_called_once()
        mock_file_tracker.record_corruption.assert_not_called()

    @patch('transfer_manager.SSHConnectionPool')
    @patch('ssh_manager.batch_get_remote_sizes')
    @patch('process_runner.execute_streaming_command', side_effect=RemoteTransferError("Rsync failed"))
    def test_rsync_failure_after_max_retries(
        self,
        mock_execute_command,
        mock_batch_get_sizes,
        MockSshPool,
        rsync_config,
        mock_file_tracker,
        fs
    ):
        """
        Tests that the function fails after the maximum number of retries and raises an exception.
        """
        # --- Setup ---
        source_path = "/remote/fail.file"
        local_path = "/local/fail.file"
        fs.create_dir("/local")

        # The execute command will always raise an error due to the side_effect

        # --- Execute & Assert ---
        with pytest.raises(RemoteTransferError):
            transfer_content_rsync(
                sftp_config=rsync_config['SOURCE_SERVER'],
                remote_path=source_path,
                local_path=local_path,
                torrent_hash="hash_fail",
                rsync_options=[],
                file_tracker=mock_file_tracker,
                total_size=1000,
                log_transfer=MagicMock(),
                _update_transfer_progress=MagicMock(),
                dry_run=False
            )

        # MAX_RETRY_ATTEMPTS is 3 in the code
        assert mock_execute_command.call_count == 3
        mock_file_tracker.record_corruption.assert_called_once_with("hash_fail", source_path)

    @patch('transfer_manager.SSHConnectionPool')
    @patch('ssh_manager.batch_get_remote_sizes')
    @patch('process_runner.execute_streaming_command')
    def test_rsync_success_after_retries(
        self,
        mock_execute_command,
        mock_batch_get_sizes,
        MockSshPool,
        rsync_config,
        mock_file_tracker,
        fs
    ):
        """
        Tests that the function succeeds if one of the retry attempts is successful.
        """
        # --- Setup ---
        source_path = "/remote/retry.file"
        local_path = "/local/retry.file"
        fs.create_dir("/local")

        # Simulate failure on the first attempt, success on the second
        def side_effect_for_retry(*args, **kwargs):
            if mock_execute_command.call_count == 1:
                raise RemoteTransferError("Rsync failed on first attempt")
            # On second call, simulate success by creating the file and returning True
            fs.create_file(local_path, contents="a" * 1000)
            return True

        mock_execute_command.side_effect = side_effect_for_retry

        # Mock the post-transfer verification
        mock_batch_get_sizes.return_value = {source_path: 1000}
        MockSshPool.return_value = MockSSHConnectionPool(host="", port=22, username="", password="")

        # --- Execute ---
        transfer_content_rsync(
            sftp_config=rsync_config['SOURCE_SERVER'],
            remote_path=source_path,
            local_path=local_path,
            torrent_hash="hash_retry",
            rsync_options=[],
            file_tracker=mock_file_tracker,
            total_size=1000,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock(),
            dry_run=False,
            rsync_timeout=60 # Explicitly set to 60 to match test expectations
        )

        # --- Assert ---
        assert mock_execute_command.call_count == 2
        mock_file_tracker.record_corruption.assert_not_called()


class TestRsyncUploadWorkflow:
    """Tests the two-stage rsync_upload workflow."""

    @pytest.fixture
    def mock_configs(self):
        """Provides mock source and destination server configs."""
        config = configparser.ConfigParser()
        config['SOURCE'] = {
            'host': 'source.host', 'port': '22', 'username': 'src_user', 'password': 'src_password'
        }
        config['DEST'] = {
            'host': 'dest.host', 'port': '22', 'username': 'dest_user', 'password': 'dest_password'
        }
        config['SETTINGS'] = {
            'local_cache_path': '/tmp/custom_cache'
        }
        return config['SOURCE'], config['DEST']

    @patch('transfer_manager._transfer_content_rsync_upload_from_cache')
    @patch('transfer_manager.transfer_content_rsync')
    @patch('shutil.rmtree')
    def test_upload_workflow_sequence_and_cleanup(
        self,
        mock_rmtree,
        mock_rsync_download,
        mock_rsync_upload_from_cache,
        mock_configs,
        mock_file_tracker,
        fs  # Add fs fixture
    ):
        """
        Verifies the correct sequence of operations: download then upload.
        Also verifies that the temporary cache directory is cleaned up on success.
        """
        # --- Setup ---
        source_config, dest_config = mock_configs
        source_path = "/remote/source/my_file"
        dest_path = "/remote/dest/my_file"

        # Determine expected cache path from mock_configs
        expected_cache_root = source_config.parser['SETTINGS']['local_cache_path']

        # --- Execute ---
        transfer_content_rsync_upload(
            source_config=source_config,
            dest_config=dest_config,
            rsync_options=[],
            source_content_path=source_path,
            dest_content_path=dest_path,
            torrent_hash="hash_upload",
            file_tracker=mock_file_tracker,
            total_size=2000,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock(),
            dry_run=False,
            is_folder=False
        )

        # --- Assert ---
        # 1. Assert download was called correctly
        mock_rsync_download.assert_called_once()
        download_args = mock_rsync_download.call_args[1]
        assert download_args['sftp_config'] == source_config
        assert download_args['remote_path'] == source_path
        assert download_args['torrent_hash'] == "hash_upload"

        # Check that the local path is inside the custom cache directory
        assert expected_cache_root in download_args['local_path']
        assert "torrent_mover_cache_hash_upload" in download_args['local_path']

        # 2. Assert upload was called correctly
        mock_rsync_upload_from_cache.assert_called_once()
        upload_args = mock_rsync_upload_from_cache.call_args[1]
        assert upload_args['dest_config'] == dest_config
        assert upload_args['remote_path'] == dest_path
        assert upload_args['torrent_hash'] == "hash_upload"
        assert expected_cache_root in upload_args['local_path']

        # 3. Assert cleanup was performed
        mock_rmtree.assert_called_once()
        # Get the path passed to rmtree and check it's the correct cache dir
        rmtree_path = mock_rmtree.call_args[0][0]
        assert expected_cache_root in str(rmtree_path)
        assert "torrent_mover_cache_hash_upload" in str(rmtree_path)

    @patch('transfer_manager.shutil.disk_usage')
    @patch('transfer_manager.transfer_content_rsync')
    def test_rsync_upload_insufficient_storage(
        self,
        mock_rsync_download,
        mock_disk_usage,
        mock_configs,
        mock_file_tracker,
        fs
    ):
        """
        Verifies that transfer is rejected if storage is insufficient.
        """
        source_config, dest_config = mock_configs

        # Set a very small free space (e.g., 1 KB)
        mock_disk_usage.return_value.free = 1024
        total_size = 200 * 1024 * 1024 # 200 MB needed

        # Ensure padding logic is triggered (100MB padding + total_size)

        with pytest.raises(RemoteTransferError) as exc:
            transfer_content_rsync_upload(
                source_config=source_config,
                dest_config=dest_config,
                rsync_options=[],
                source_content_path="/remote/source",
                dest_content_path="/remote/dest",
                torrent_hash="hash_storage",
                file_tracker=mock_file_tracker,
                total_size=total_size,
                log_transfer=MagicMock(),
                _update_transfer_progress=MagicMock(),
                dry_run=False,
                is_folder=False
            )

        assert "Insufficient space" in str(exc.value)
        mock_rsync_download.assert_not_called()

    def test_rsync_upload_progress_weighting(
        self,
        mock_configs,
        mock_file_tracker,
        fs
    ):
        """
        Verifies the progress weighting logic (50% DL, 50% UL).
        """
        source_config, dest_config = mock_configs
        main_callback = MagicMock()
        total_size = 200
        doubled_total = 400

        with patch('transfer_manager.transfer_content_rsync') as mock_dl, \
             patch('transfer_manager._transfer_content_rsync_upload_from_cache') as mock_ul, \
             patch('shutil.rmtree'):

            # Simulate DL and UL functions calling their progress callback
            def dl_side_effect(**kwargs):
                callback = kwargs['_update_transfer_progress']
                # Simulate 100 bytes transferred out of 200 (50% of DL phase)
                # rsync progress=0.5, bytes=100
                callback("hash", 0.5, 100, 200)

            mock_dl.side_effect = dl_side_effect

            def ul_side_effect(**kwargs):
                callback = kwargs['_update_transfer_progress']
                # Simulate 100 bytes transferred out of 200 (50% of UL phase)
                # rsync progress=0.5, bytes=100
                callback("hash", 0.5, 100, 200)

            mock_ul.side_effect = ul_side_effect

            transfer_content_rsync_upload(
                source_config=source_config,
                dest_config=dest_config,
                rsync_options=[],
                source_content_path="/src",
                dest_content_path="/dest",
                torrent_hash="hash",
                file_tracker=mock_file_tracker,
                total_size=total_size,
                log_transfer=MagicMock(),
                _update_transfer_progress=main_callback,
                dry_run=False,
                is_folder=False
            )

            calls = main_callback.call_args_list

            # 1. Verify DL call:
            # Input to wrapper: progress=0.5, transferred=100
            # Wrapper logic: weighted = 0.5 * 0.5 = 0.25
            #                cumulative = 100
            dl_call_found = False
            for c in calls:
                args = c[0]
                if args[1] == 0.25 and args[2] == 100 and args[3] == doubled_total:
                    dl_call_found = True
                    break
            assert dl_call_found, "Main callback not called with correct DL values"

            # 2. Verify UL call:
            # Input to wrapper: progress=0.5, transferred=100
            # Wrapper logic: weighted = 0.5 + (0.5 * 0.5) = 0.75
            #                cumulative = total_size + transferred = 200 + 100 = 300
            ul_call_found = False
            for c in calls:
                args = c[0]
                if args[1] == 0.75 and args[2] == 300 and args[3] == doubled_total:
                    ul_call_found = True
                    break
            assert ul_call_found, "Main callback not called with correct UL values"

    @patch('transfer_manager._transfer_content_rsync_upload_from_cache')
    @patch('transfer_manager.transfer_content_rsync', side_effect=RemoteTransferError("Download failed"))
    @patch('shutil.rmtree')
    def test_upload_workflow_cleanup_even_on_failure(
        self,
        mock_rmtree,
        mock_rsync_download,
        mock_rsync_upload_from_cache,
        mock_configs,
        mock_file_tracker,
        fs  # Add fs fixture
    ):
        """
        Verifies that cache IS cleaned up even if the initial download phase fails.
        """
        # --- Setup ---
        source_config, dest_config = mock_configs

        # --- Execute & Assert ---
        with pytest.raises(RemoteTransferError):
            transfer_content_rsync_upload(
                source_config=source_config,
                dest_config=dest_config,
                rsync_options=[],
                source_content_path="/remote/source/file",
                dest_content_path="/remote/dest/file",
                torrent_hash="hash_fail_dl",
                file_tracker=mock_file_tracker,
                total_size=1000,
                log_transfer=MagicMock(),
                _update_transfer_progress=MagicMock(),
                dry_run=False,
                is_folder=False
            )

        # Assert primary failure behavior
        mock_rsync_download.assert_called_once()
        mock_rsync_upload_from_cache.assert_not_called()

        # Assert that cleanup WAS performed (Robust Cleanup)
        mock_rmtree.assert_called_once()


    @patch('process_runner.execute_streaming_command', return_value=True)
    def test_upload_from_cache_command_construction(
        self,
        mock_execute_command,
        mock_configs,
        fs
    ):
        """
        Verifies that _transfer_content_rsync_upload_from_cache constructs the
        correct rsync command for the upload direction (local -> remote).
        """
        # --- Setup ---
        _, dest_config = mock_configs
        local_path = "/cache/my_upload_file"
        remote_path = "/remote/dest/my_upload_file"
        remote_parent = "/remote/dest"

        fs.create_file(local_path, contents="some data")

        # --- Execute ---
        _transfer_content_rsync_upload_from_cache(
            dest_config=dest_config,
            local_path=local_path,
            remote_path=remote_path,
            torrent_hash="hash_upload_cmd",
            rsync_options=["--custom-flag"],
            total_size=100,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock(),
            dry_run=False
        )

        # --- Assert ---
        mock_execute_command.assert_called_once()
        called_command = mock_execute_command.call_args[0][0]

        # 1. Verify direction: local source, remote destination
        assert local_path in called_command
        expected_remote_spec = f"dest_user@dest.host:{remote_parent}"
        assert expected_remote_spec in called_command

        # 2. Verify essential flags
        assert "rsync" in called_command
        assert "--info=progress2" in called_command
        assert "--custom-flag" in called_command # User option

        # Assert data-only flags
        assert "--no-times" in called_command
        assert "--no-perms" in called_command
        assert "--no-owner" in called_command
        assert "--no-group" in called_command

        # Assert --timeout is NOT present
        assert "--timeout" not in " ".join(called_command)

        # 3. Verify sshpass usage
        assert "sshpass" in called_command
        assert "-p" in called_command
        assert "dest_password" in called_command

class TestRsyncAdaptiveTimeout:
    @patch('transfer_manager.SSHConnectionPool')
    @patch('ssh_manager.batch_get_remote_sizes')
    @patch('process_runner.execute_streaming_command')
    @patch('time.sleep')
    def test_adaptive_timeout_backoff(
        self,
        mock_sleep,
        mock_execute_command,
        mock_batch_get_sizes,
        MockSshPool,
        rsync_config,
        mock_file_tracker,
        fs
    ):
        """
        Verifies that retries use increasing timeout values.
        """
        # --- Setup ---
        source_path = "/remote/file"
        local_path = "/local/file"
        fs.create_dir("/local")
        mock_batch_get_sizes.return_value = {source_path: 1000}
        MockSshPool.return_value = MockSSHConnectionPool(host="", port=22, username="", password="")

        # First call fails with timeout, second succeeds
        def side_effect(*args, **kwargs):
            timeout = kwargs.get('timeout_seconds')
            if timeout == 60:
                raise RemoteTransferError("Timeout")
            elif timeout == 120: # 60 + 60
                fs.create_file(local_path, contents="a" * 1000)
                return True
            return False

        mock_execute_command.side_effect = side_effect

        # --- Execute ---
        transfer_content_rsync(
            sftp_config=rsync_config['SOURCE_SERVER'],
            remote_path=source_path,
            local_path=local_path,
            torrent_hash="hash_adaptive",
            rsync_options=[],
            file_tracker=mock_file_tracker,
            total_size=1000,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock(),
            dry_run=False,
            rsync_timeout=60 # Explicitly set to 60 to match test expectations
        )

        # --- Assert ---
        assert mock_execute_command.call_count == 2

        # Check first call had timeout=60
        args1, kwargs1 = mock_execute_command.call_args_list[0]
        assert kwargs1['timeout_seconds'] == 60

        # Check second call had timeout=120
        args2, kwargs2 = mock_execute_command.call_args_list[1]
        assert kwargs2['timeout_seconds'] == 120

        # Verify sleep was called
        mock_sleep.assert_called_with(10)