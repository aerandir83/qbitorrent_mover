import pytest
from unittest.mock import MagicMock, patch, call
import configparser
import transfer_manager
from transfer_manager import transfer_content_rsync, _transfer_content_rsync_upload_from_cache, RemoteTransferError

@pytest.fixture
def mock_config():
    config = configparser.ConfigParser()
    config['SOURCE'] = {'host': 'src', 'port': '22', 'username': 'u', 'password': 'p'}
    config['DEST'] = {'host': 'dst', 'port': '22', 'username': 'u', 'password': 'p'}
    return config

@pytest.fixture
def mock_tracker():
    t = MagicMock()
    t.is_corrupted.return_value = False
    return t

@patch('process_runner.execute_streaming_command', return_value=True)
@patch('ssh_manager.SSHConnectionPool')
@patch('ssh_manager.batch_get_remote_sizes')
@patch('transfer_manager.SpeedMonitor')
@patch('transfer_manager._get_ssh_command', return_value="ssh -p 22")
class TestRsyncOpt:

    def test_signature_and_defaults(self, mock_get_ssh, mock_monitor_cls, mock_sizes, mock_pool, mock_exec, mock_config, mock_tracker, fs):
        """Verify default behavior: No checksum, yes ciphers."""
        mock_pool.return_value.get_connection.return_value.__enter__.return_value = (MagicMock(), MagicMock())
        fs.create_dir('/local')
        mock_sizes.return_value = {'/remote': 100}

        # Mock file creation to satisfy verification
        def side_effect(*args, **kwargs):
            fs.create_file('/local/file', contents='a'*100)
            return True
        mock_exec.side_effect = side_effect

        transfer_content_rsync(
            sftp_config=mock_config['SOURCE'],
            remote_path='/remote',
            local_path='/local/file',
            torrent_hash='hash',
            rsync_options=['-a'],
            file_tracker=mock_tracker,
            total_size=100,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock(),
            ui=MagicMock(),
            dry_run=False,
            # force_integrity_check should default to False
        )

        cmd = mock_exec.call_args[0][0]
        assert '--checksum' not in cmd
        assert '-c' not in cmd

        e_idx = cmd.index('-e')
        ssh_cmd = cmd[e_idx+1]
        assert "-c aes128-gcm@openssh.com,aes128-ctr" in ssh_cmd

    def test_force_integrity_check_enabled(self, mock_get_ssh, mock_monitor_cls, mock_sizes, mock_pool, mock_exec, mock_config, mock_tracker, fs):
        """Verify force_integrity_check=True adds checksum."""
        mock_pool.return_value.get_connection.return_value.__enter__.return_value = (MagicMock(), MagicMock())
        fs.create_dir('/local')
        mock_sizes.return_value = {'/remote': 100}

        def side_effect(*args, **kwargs):
            fs.create_file('/local/file', contents='a'*100)
            return True
        mock_exec.side_effect = side_effect

        transfer_content_rsync(
            sftp_config=mock_config['SOURCE'],
            remote_path='/remote',
            local_path='/local/file',
            torrent_hash='hash',
            rsync_options=['-a'],
            file_tracker=mock_tracker,
            total_size=100,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock(),
            ui=MagicMock(),
            dry_run=False,
            force_integrity_check=True # Explicitly True
        )

        cmd = mock_exec.call_args[0][0]
        assert '--checksum' in cmd

    def test_smart_retry_logic(self, mock_get_ssh, mock_monitor_cls, mock_sizes, mock_pool, mock_exec, mock_config, mock_tracker, fs):
        """Verify verification failure triggers retry with integrity check."""
        mock_pool.return_value.get_connection.return_value.__enter__.return_value = (MagicMock(), MagicMock())
        fs.create_dir('/local')

        # Scenario:
        # 1. First attempt: Success exec, but Size Mismatch (Remote=100, Local=50)
        # 2. Smart Retry: Recursive call with force_integrity_check=True
        # 3. Second attempt: Success exec, Size Match (Remote=100, Local=100)

        mock_sizes.return_value = {'/remote': 100}

        # We need side_effect to simulate the state change between calls
        # call_count is 0 initially.
        def exec_side_effect(*args, **kwargs):
            # args[0] is the command list. Check if checksum is present to determine which attempt this is
            cmd = args[0]
            if '--checksum' not in cmd:
                # First attempt: Create partial file (fail verification)
                if fs.exists('/local/file'): fs.remove('/local/file')
                fs.create_file('/local/file', contents='a'*50)
            else:
                # Second attempt (Smart Retry): Create full file (pass verification)
                if fs.exists('/local/file'): fs.remove('/local/file')
                fs.create_file('/local/file', contents='a'*100)
            return True

        mock_exec.side_effect = exec_side_effect

        transfer_content_rsync(
            sftp_config=mock_config['SOURCE'],
            remote_path='/remote',
            local_path='/local/file',
            torrent_hash='hash',
            rsync_options=['-a'],
            file_tracker=mock_tracker,
            total_size=100,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock(),
            ui=MagicMock(),
            dry_run=False,
            force_integrity_check=False
        )

        assert mock_exec.call_count == 2

        # First call: No checksum
        cmd1 = mock_exec.call_args_list[0][0][0]
        assert '--checksum' not in cmd1

        # Second call: Yes checksum
        cmd2 = mock_exec.call_args_list[1][0][0]
        assert '--checksum' in cmd2

    def test_ui_decoupling(self, mock_get_ssh, mock_monitor_cls, mock_sizes, mock_pool, mock_exec, mock_config, mock_tracker, fs):
        """Verify UI methods are not called."""
        mock_pool.return_value.get_connection.return_value.__enter__.return_value = (MagicMock(), MagicMock())
        fs.create_dir('/local')
        mock_sizes.return_value = {'/remote': 100}

        mock_monitor_instance = MagicMock()
        mock_monitor_instance.get_status.return_value = {'history': [], 'current_speed': 0}
        mock_monitor_cls.return_value = mock_monitor_instance

        ui = MagicMock()

        # Mock success to avoid retries
        mock_exec.return_value = True
        def side_effect(*args, **kwargs):
            fs.create_file('/local/file', contents='a'*100)
            return True
        mock_exec.side_effect = side_effect

        transfer_content_rsync(
            sftp_config=mock_config['SOURCE'],
            remote_path='/remote',
            local_path='/local/file',
            torrent_hash='hash',
            rsync_options=['-a'],
            file_tracker=mock_tracker,
            total_size=100,
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock(),
            ui=ui,
            dry_run=False
        )

        # Invoke callback
        call_kwargs = mock_monitor_cls.call_args[1]
        callback = call_kwargs.get('callback')
        if callback:
            callback(1, 1, 1)

        ui.update_current_speed.assert_not_called()
        ui.update_speed_history.assert_not_called()
