import pytest
from unittest.mock import MagicMock, patch
import subprocess
from system_manager import change_ownership
from torrent_mover import _post_transfer_actions
from ssh_manager import SSHConnectionPool

# --- Tests for system_manager.change_ownership ---

def test_change_ownership_local_success():
    with patch('subprocess.run') as mock_run, patch('shutil.which') as mock_which:
        mock_which.return_value = "/usr/bin/chown"
        mock_run.return_value.returncode = 0

        result = change_ownership("/path/to/file", "user", "group")

        assert result is True
        mock_run.assert_called_once()

def test_change_ownership_local_failure():
    with patch('subprocess.run') as mock_run, patch('shutil.which') as mock_which:
        mock_which.return_value = "/usr/bin/chown"
        mock_run.return_value.returncode = 1

        result = change_ownership("/path/to/file", "user", "group")

        assert result is False
        mock_run.assert_called_once()

def test_change_ownership_local_no_chown_command():
    with patch('shutil.which') as mock_which:
        mock_which.return_value = None

        result = change_ownership("/path/to/file", "user", "group")

        assert result is False

def test_change_ownership_remote_success():
    mock_pool = MagicMock(spec=SSHConnectionPool)
    mock_ssh = MagicMock()
    mock_stdout = MagicMock()
    mock_stdout.channel.recv_exit_status.return_value = 0
    mock_ssh.exec_command.return_value = (None, mock_stdout, MagicMock())

    context_manager = MagicMock()
    context_manager.__enter__.return_value = (None, mock_ssh)
    mock_pool.get_connection.return_value = context_manager

    pools = {'DESTINATION_SERVER': mock_pool}
    remote_config = MagicMock()

    result = change_ownership("/path/to/file", "user", "group", remote_config=remote_config, ssh_connection_pools=pools)

    assert result is True
    mock_ssh.exec_command.assert_called_once()

def test_change_ownership_remote_failure():
    mock_pool = MagicMock(spec=SSHConnectionPool)
    mock_ssh = MagicMock()
    mock_stdout = MagicMock()
    mock_stdout.channel.recv_exit_status.return_value = 1
    mock_stderr = MagicMock()
    mock_stderr.read.return_value = b"Error"
    mock_ssh.exec_command.return_value = (None, mock_stdout, mock_stderr)

    context_manager = MagicMock()
    context_manager.__enter__.return_value = (None, mock_ssh)
    mock_pool.get_connection.return_value = context_manager

    pools = {'DESTINATION_SERVER': mock_pool}
    remote_config = MagicMock()

    result = change_ownership("/path/to/file", "user", "group", remote_config=remote_config, ssh_connection_pools=pools)

    assert result is False
    mock_ssh.exec_command.assert_called_once()

def test_change_ownership_no_op():
    result = change_ownership("/path/to/file", "", "")
    assert result is True

def test_change_ownership_dry_run():
    result = change_ownership("/path/to/file", "user", "group", dry_run=True)
    assert result is True


# --- Tests for torrent_mover._post_transfer_actions ---

@patch('torrent_mover.change_ownership')
def test_post_transfer_halts_on_chown_failure(mock_change_ownership):
    # Setup
    mock_change_ownership.return_value = False

    mock_torrent = MagicMock()
    mock_torrent.name = "Test Torrent"
    mock_torrent.hash = "hash123"

    mock_config = MagicMock()
    # Need to handle get() method for boolean check in .get('chown_user', '')
    mock_config.__getitem__.side_effect = lambda key: {
        'SETTINGS': {'transfer_mode': 'sftp', 'chown_user': 'u', 'chown_group': 'g'},
        'DESTINATION_SERVER': {},
        'DESTINATION_PATHS': {'destination_path': '/dest', 'remote_destination_path': '/remote'}
    }.get(key, {})

    # Execute
    result, msg = _post_transfer_actions(
        torrent=mock_torrent,
        source_qbit=MagicMock(),
        destination_qbit=MagicMock(),
        config=mock_config,
        tracker_rules={},
        ssh_connection_pools={},
        dest_content_path="/dest/content",
        destination_save_path="/remote/content",
        transfer_executed=True,
        dry_run=False,
        test_run=False,
        file_tracker=MagicMock(),
        transfer_mode="sftp",
        all_files=[],
        ui=MagicMock(),
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock()
    )

    # Assert
    assert result is False
    assert "Ownership change failed" in msg
    mock_change_ownership.assert_called_once()

@patch('torrent_mover.change_ownership')
def test_post_transfer_continues_on_chown_success(mock_change_ownership):
    # Setup
    mock_change_ownership.return_value = True

    mock_torrent = MagicMock()
    mock_torrent.name = "Test Torrent"
    mock_torrent.hash = "hash123"

    mock_config = MagicMock()
    mock_config.__getitem__.side_effect = lambda key: {
        'SETTINGS': {'transfer_mode': 'sftp', 'chown_user': 'u', 'chown_group': 'g'},
        'DESTINATION_SERVER': {},
        'DESTINATION_PATHS': {'destination_path': '/dest', 'remote_destination_path': '/remote'},
        'DESTINATION_CLIENT': {'add_torrents_paused': 'false', 'start_torrents_after_recheck': 'false'},
        'SOURCE_CLIENT': {'delete_after_transfer': 'false'}
    }.get(key, {})
    mock_config.getboolean.return_value = False

    mock_dest_qbit = MagicMock()
    mock_dest_qbit.torrents_info.return_value = [] # Simulate torrent not present

    # Mock recheck wait to return SUCCESS so we don't hit other logic
    with patch('torrent_mover.wait_for_recheck_completion', return_value="SUCCESS"):
        # Execute
        result, msg = _post_transfer_actions(
            torrent=mock_torrent,
            source_qbit=MagicMock(),
            destination_qbit=mock_dest_qbit,
            config=mock_config,
            tracker_rules={},
            ssh_connection_pools={},
            dest_content_path="/dest/content",
            destination_save_path="/remote/content",
            transfer_executed=True,
            dry_run=False,
            test_run=False,
            file_tracker=MagicMock(),
            transfer_mode="sftp",
            all_files=[],
            ui=MagicMock(),
            log_transfer=MagicMock(),
            _update_transfer_progress=MagicMock()
        )

    # Assert
    assert result is True
    mock_change_ownership.assert_called_once()
