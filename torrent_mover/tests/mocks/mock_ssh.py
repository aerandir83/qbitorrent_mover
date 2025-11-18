from contextlib import contextmanager
import paramiko
from typing import Generator, Tuple, Dict, Any

class MockSFTPClient:
    """Mock for paramiko.SFTPClient."""
    def __init__(self):
        self.files: Dict[str, Any] = {} # Mock filesystem
        pass

    def stat(self, path: str):
        # Mock stat
        pass

    def open(self, path: str, mode: str):
        # Mock open
        pass

    def remove(self, path: str):
        # Mock remove
        pass

    def mkdir(self, path: str, mode: int = 0o777):
        # Mock mkdir
        pass

class MockSSHClient:
    """Mock for paramiko.SSHClient."""
    def __init__(self):
        pass

    def get_transport(self):
        # Mock transport
        return self

    def is_active(self):
        # Mock is_active
        return True

    def open_sftp(self) -> MockSFTPClient:
        return MockSFTPClient()

    def exec_command(self, command: str, timeout: int):
        # Mock exec_command
        pass

    def close(self):
        # Mock close
        pass

class MockSSHConnectionPool:
    """Mock for torrent_mover.ssh_manager.SSHConnectionPool."""
    def __init__(self, host: str, port: int, username: str, password: str, **kwargs):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.mock_ssh_client = MockSSHClient()
        self.mock_sftp_client = self.mock_ssh_client.open_sftp()

    @contextmanager
    def get_connection(self) -> Generator[Tuple[MockSFTPClient, MockSSHClient], None, None]:
        # Yield the mock clients
        try:
            yield (self.mock_sftp_client, self.mock_ssh_client)
        finally:
            # No-op on close
            pass

    def close_all(self):
        pass
