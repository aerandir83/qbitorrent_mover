import pytest
from unittest.mock import MagicMock, call
from system_manager import force_remote_permissions
from ssh_manager import SSHConnectionPool

def test_force_remote_permissions_success(mocker):
    mock_pool = MagicMock(spec=SSHConnectionPool)
    mock_ssh = MagicMock()
    mock_sftp = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = (mock_sftp, mock_ssh)

    mock_stdout = MagicMock()
    mock_stdout.channel.recv_exit_status.return_value = 0
    mock_ssh.exec_command.return_value = (None, mock_stdout, None)

    result = force_remote_permissions("/path/to/files", mock_pool)

    assert result is True
    mock_ssh.exec_command.assert_called_once()
    args, _ = mock_ssh.exec_command.call_args
    assert "chmod -R 777" in args[0]
    assert "/path/to/files" in args[0]

def test_force_remote_permissions_spaces(mocker):
    mock_pool = MagicMock(spec=SSHConnectionPool)
    mock_ssh = MagicMock()
    mock_sftp = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = (mock_sftp, mock_ssh)

    mock_stdout = MagicMock()
    mock_stdout.channel.recv_exit_status.return_value = 0
    mock_ssh.exec_command.return_value = (None, mock_stdout, None)

    result = force_remote_permissions("/path/to/files with spaces", mock_pool)

    assert result is True
    mock_ssh.exec_command.assert_called_once()
    args, _ = mock_ssh.exec_command.call_args
    assert "chmod -R 777" in args[0]
    assert "'/path/to/files with spaces'" in args[0]

def test_force_remote_permissions_failure(mocker):
    mock_pool = MagicMock(spec=SSHConnectionPool)
    mock_ssh = MagicMock()
    mock_sftp = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = (mock_sftp, mock_ssh)

    mock_stdout = MagicMock()
    mock_stdout.channel.recv_exit_status.return_value = 1

    mock_stderr = MagicMock()
    mock_stderr.read.return_value = b"Permission denied"

    mock_ssh.exec_command.return_value = (None, mock_stdout, mock_stderr)

    result = force_remote_permissions("/path/to/files", mock_pool)

    assert result is False

def test_force_remote_permissions_no_such_file(mocker):
    mock_pool = MagicMock(spec=SSHConnectionPool)
    mock_ssh = MagicMock()
    mock_sftp = MagicMock()
    mock_pool.get_connection.return_value.__enter__.return_value = (mock_sftp, mock_ssh)

    mock_stdout = MagicMock()
    mock_stdout.channel.recv_exit_status.return_value = 1

    mock_stderr = MagicMock()
    mock_stderr.read.return_value = b"No such file or directory"

    mock_ssh.exec_command.return_value = (None, mock_stdout, mock_stderr)

    result = force_remote_permissions("/path/to/files", mock_pool)

    # Should return True for "no such file"
    assert result is True
