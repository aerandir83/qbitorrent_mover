"""Manages SSH and SFTP connections and remote file operations.

This module provides the core functionality for interacting with remote servers
over SSH and SFTP. It includes:
- A thread-safe connection pool (`SSHConnectionPool`) for reusing SSH/SFTP
  connections, which significantly improves performance by reducing the
  overhead of repeated authentication.
- Utility functions for common remote operations, such as checking if a remote
  path is a directory (`is_remote_dir`), recursively creating directories
  (`sftp_mkdir_p`), and efficiently calculating the size of remote files and
  directories (`batch_get_remote_sizes`).
- Prerequisite checks for external dependencies like `sshpass`.
- Configuration for SSH connection multiplexing to further speed up operations.
"""
import paramiko
import logging
import threading
from queue import Queue, Empty
from contextlib import contextmanager
import typing
import shlex
import os
import subprocess
import shutil
import tempfile
import getpass
import time

from .utils import retry

# --- Constants ---
DEFAULT_KEEPALIVE_INTERVAL = 30
DEFAULT_SSH_POOL_SIZE = 5
SSH_EXEC_TIMEOUT = 60
BATCH_SSH_EXEC_TIMEOUT = 120
BATCH_SIZE = 10
MAX_COMMAND_LENGTH = 100000  # Conservative limit for shell command length
MAX_RETRY_ATTEMPTS = 2
RETRY_DELAY_SECONDS = 5

SSH_CONTROL_PATH: typing.Optional[str] = None

def setup_ssh_control_path() -> None:
    """Creates a user-specific directory for SSH control sockets to enable multiplexing.

    This function sets up a directory within the system's temporary folder to
    store SSH control sockets. Using a control path allows SSH to reuse an
    existing connection for subsequent sessions to the same host, which can
    dramatically speed up operations like `rsync` that make multiple SSH calls.
    The global `SSH_CONTROL_PATH` variable is set to the socket path template,
    which is then used by `_get_ssh_command`.
    """
    global SSH_CONTROL_PATH
    try:
        user = getpass.getuser()
        control_dir = os.path.join(tempfile.gettempdir(), f"torrent_mover_ssh_{user}")
        os.makedirs(control_dir, mode=0o700, exist_ok=True)
        SSH_CONTROL_PATH = os.path.join(control_dir, "%r@%h:%p")
        logging.debug(f"Using SSH control path for multiplexing: {SSH_CONTROL_PATH}")
    except Exception as e:
        logging.warning(f"Could not create SSH control path directory. Multiplexing will be disabled. Error: {e}")
        SSH_CONTROL_PATH = None

def check_sshpass_installed() -> None:
    """Checks if the `sshpass` utility is installed and executable.

    This function is a prerequisite for using `rsync` with password authentication.
    If `sshpass` is not found in the system's PATH, it logs a fatal error and
    exits the application, as `rsync` transfers would fail. It also triggers the
    setup for SSH multiplexing.

    Raises:
        SystemExit: If the `sshpass` command is not found.
    """
    if shutil.which("sshpass") is None:
        logging.error("FATAL: 'sshpass' is not installed or not in the system's PATH.")
        logging.error("Please install 'sshpass' to use the rsync transfer mode with a password.")
        logging.error("e.g., 'sudo apt-get install sshpass' or 'sudo yum install sshpass'")
        exit(1)
    logging.debug("'sshpass' dependency check passed.")
    setup_ssh_control_path()

def _get_ssh_command(port: int) -> str:
    """Builds the base SSH command string for use with `rsync`.

    This function constructs the `-e` argument for `rsync`. It includes optimized
    cipher settings and options to ignore host key checking for automated use.
    If SSH multiplexing is enabled via `setup_ssh_control_path`, it adds the
    necessary `ControlMaster` options.

    Args:
        port: The SSH port number for the connection.

    Returns:
        A string containing the formatted SSH command.
    """
    base_ssh_cmd = f"ssh -p {port} -c aes128-ctr -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=15"
    if SSH_CONTROL_PATH:
        multiplex_opts = f"-o ControlMaster=auto -o ControlPath={shlex.quote(SSH_CONTROL_PATH)} -o ControlPersist=60s"
        return f"{base_ssh_cmd} {multiplex_opts}"
    return base_ssh_cmd

def sftp_mkdir_p(sftp: paramiko.SFTPClient, remote_path: str) -> None:
    """Recursively creates a directory path on an SFTP server, similar to `mkdir -p`.

    This function ensures that an entire directory structure exists on the remote
    server. It checks for the existence of each parent directory in the path and
    creates it if it is missing.

    Args:
        sftp: An active Paramiko `SFTPClient` object.
        remote_path: The absolute path of the directory to create on the remote server.

    Raises:
        IOError: If a directory could not be created and was confirmed to not exist.
    """
    if not remote_path:
        return
    remote_path = remote_path.replace('\\', '/').rstrip('/')
    try:
        sftp.stat(remote_path)
    except FileNotFoundError:
        parent_dir = os.path.dirname(remote_path)
        sftp_mkdir_p(sftp, parent_dir)
        try:
            sftp.mkdir(remote_path)
        except IOError as e:
            logging.error(f"Failed to create remote directory '{remote_path}': {e}")
            # Re-check existence in case of a race condition
            try:
                sftp.stat(remote_path)
            except FileNotFoundError:
                raise e

def is_remote_dir(ssh_client: paramiko.SSHClient, path: str) -> bool:
    """Checks if a given path on a remote server corresponds to a directory.

    This function executes the `test -d` command over an SSH connection for an
    efficient, low-overhead check.

    Args:
        ssh_client: An active Paramiko `SSHClient` object.
        path: The absolute path to check on the remote server.

    Returns:
        `True` if the path is a directory, `False` otherwise or if an error occurs.
    """
    logging.debug(f"Checking if remote path is directory: {path}")
    try:
        command = f"test -d {shlex.quote(path)}"
        _stdin, stdout, _stderr = ssh_client.exec_command(command, timeout=SSH_EXEC_TIMEOUT)
        exit_status = stdout.channel.recv_exit_status()
        return exit_status == 0
    except Exception as e:
        logging.error(f"Cannot access remote path: {path}. Details: {e}")
        return False

def _get_all_files_recursive(sftp: paramiko.SFTPClient, remote_path: str, base_dest_path: str, file_list: typing.List[typing.Tuple[str, str, int]]) -> None:
    """Recursively scans a remote directory and builds a flat list of all files.

    This is an internal helper function used by SFTP transfer strategies to get a
    complete list of files within a directory and their corresponding destination
    paths and sizes.

    Args:
        sftp: An active Paramiko `SFTPClient` object.
        remote_path: The current remote directory to scan.
        base_dest_path: The corresponding destination path for the current `remote_path`.
        file_list: A list to which tuples of (source_path, dest_path, size) will be appended.
    """
    remote_path_norm = remote_path.replace('\\', '/')
    base_dest_path_norm = base_dest_path.replace('\\', '/')
    try:
        items = sftp.listdir(remote_path_norm)
    except FileNotFoundError:
        logging.warning(f"Directory not found on source, skipping: {remote_path_norm}")
        return
    for item in items:
        remote_item_path = f"{remote_path_norm.rstrip('/')}/{item}"
        dest_item_path = f"{base_dest_path_norm.rstrip('/')}/{item}"
        try:
            stat_info = sftp.stat(remote_item_path)
            if stat_info.st_mode & 0o40000:  # Check if it is a directory (S_ISDIR)
                _get_all_files_recursive(sftp, remote_item_path, dest_item_path, file_list)
            else:
                file_list.append((remote_item_path, dest_item_path, stat_info.st_size))
        except FileNotFoundError:
            logging.warning(f"File or directory vanished during scan, skipping: {remote_item_path}")
            continue

class SSHConnectionPool:
    """A thread-safe pool for managing and reusing Paramiko SSH/SFTP connections.

    This class provides a mechanism to reuse SSH connections, which significantly
    reduces the overhead of authentication for each file transfer. It handles the
    creation, validation, and recycling of connections in a thread-safe manner,
    making it suitable for use with parallel transfers.

    Attributes:
        host (str): The hostname or IP address of the SSH server.
        port (int): The port number of the SSH server.
        username (str): The username for authentication.
        password (str): The password for authentication.
        max_size (int): The maximum number of concurrent connections allowed in the pool.
        connect_timeout (int): Timeout in seconds for establishing a new connection.
        pool_wait_timeout (int): Timeout in seconds for waiting to get a connection
            from the pool when it is full.
    """
    def __init__(self, host: str, port: int, username: str, password: str,
                 max_size: int = DEFAULT_SSH_POOL_SIZE, connect_timeout: int = 10, pool_wait_timeout: int = 120):
        """Initializes the SSHConnectionPool.

        Args:
            host: The hostname of the SSH server.
            port: The port of the SSH server.
            username: The username for authentication.
            password: The password for authentication.
            max_size: The maximum number of connections to maintain in the pool.
            connect_timeout: Timeout for establishing a new SSH connection.
            pool_wait_timeout: Timeout for waiting for a connection to become available from the pool.
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.max_size = max_size
        self.connect_timeout = connect_timeout
        self.pool_wait_timeout = pool_wait_timeout
        self._pool: Queue[typing.Tuple[paramiko.SFTPClient, paramiko.SSHClient]] = Queue(maxsize=max_size)
        self._lock = threading.Lock()
        self._active_connections = 0
        self._condition = threading.Condition(self._lock)
        self._closed = False
        logging.debug(f"Initialized SSHConnectionPool for {host} with max_size={max_size}")

    def _create_connection(self) -> typing.Tuple[paramiko.SFTPClient, paramiko.SSHClient]:
        """Creates a new SSH client, establishes a connection, and opens an SFTP session."""
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                timeout=self.connect_timeout
            )
            transport = ssh_client.get_transport()
            if transport:
                transport.set_keepalive(DEFAULT_KEEPALIVE_INTERVAL)
            sftp = ssh_client.open_sftp()
            logging.debug(f"Successfully created new SSH connection to {self.host}")
            return sftp, ssh_client
        except Exception as e:
            logging.error(f"Failed to create SSH connection to {self.host}:{self.port}: {e}")
            raise

    def _is_connection_alive(self, ssh: paramiko.SSHClient) -> bool:
        """Checks if an SSH connection is still active."""
        try:
            transport = ssh.get_transport()
            return transport is not None and transport.is_active()
        except Exception:
            return False

    @contextmanager
    def get_connection(self) -> typing.Generator[typing.Tuple[paramiko.SFTPClient, paramiko.SSHClient], None, None]:
        """Provides a connection from the pool within a context manager.

        This method handles the logic of acquiring a connection (by reusing an
        existing one or creating a new one if the pool is not full) and returning
        it to the pool when the context is exited. It automatically discards and
        replaces dead connections.

        Yields:
            A tuple containing an active `(SFTPClient, SSHClient)`.

        Raises:
            RuntimeError: If the pool has already been closed.
            TimeoutError: If waiting for a connection exceeds `pool_wait_timeout`.
        """
        if self._closed:
            raise RuntimeError("Connection pool is closed.")

        sftp, ssh = None, None
        created_new = False

        try:
            while True:
                if self._closed:
                    raise RuntimeError("Connection pool was closed while waiting for a connection.")
                try:
                    sftp, ssh = self._pool.get_nowait()
                    if self._is_connection_alive(ssh):
                        logging.debug(f"Reusing existing SSH connection to {self.host}")
                        break
                    else:
                        logging.debug(f"Discarding dead SSH connection to {self.host}")
                        try:
                            ssh.close()
                        except Exception:
                            pass
                        with self._lock:
                            self._active_connections -= 1
                            self._condition.notify()
                        sftp, ssh = None, None
                        continue
                except Empty:
                    with self._condition:
                        if self._active_connections < self.max_size:
                            self._active_connections += 1
                            created_new = True
                            logging.debug(f"Creating new connection to {self.host} ({self._active_connections}/{self.max_size})")
                            break
                        else:
                            logging.debug(f"Pool full ({self.max_size}/{self.max_size}), waiting for connection to {self.host}")
                            if not self._condition.wait(timeout=self.pool_wait_timeout):
                                raise TimeoutError(f"Timeout waiting for an available SSH connection to {self.host}")

            if created_new:
                try:
                    sftp, ssh = self._create_connection()
                except Exception:
                    with self._lock:
                        self._active_connections -= 1
                        self._condition.notify()
                    raise

            yield sftp, ssh
        finally:
            if sftp and ssh:
                try:
                    self._pool.put_nowait((sftp, ssh))
                    logging.debug(f"Returned connection to pool for {self.host}. (Pool size: {self._pool.qsize()})")
                except Exception:
                    logging.warning(f"Could not return connection to full pool for {self.host}. Closing it.")
                    with self._lock:
                        self._active_connections -= 1
                        self._condition.notify()
                    try:
                        ssh.close()
                    except Exception:
                        pass

    def close_all(self) -> None:
        """Closes all active and pooled connections gracefully.

        This method should be called during application shutdown to ensure all
        SSH resources are properly cleaned up.
        """
        logging.debug(f"Closing all SSH connections for {self.host}...")
        self._closed = True
        with self._condition:
            self._condition.notify_all()
        while not self._pool.empty():
            try:
                _sftp, ssh = self._pool.get_nowait()
                ssh.close()
            except Empty:
                break
        logging.debug(f"Connection pool for {self.host} closed.")

    def get_stats(self) -> typing.Dict[str, int]:
        """Returns a dictionary with current statistics about the connection pool.

        Returns:
            A dictionary containing the number of active connections, max pool size,
            connections currently in the pool, and connections currently in use.
        """
        with self._lock:
            in_pool = self._pool.qsize()
            in_use = self._active_connections - in_pool
            return {
                "active_connections": self._active_connections,
                "max_size": self.max_size,
                "in_pool": in_pool,
                "in_use": in_use
            }

@retry(tries=MAX_RETRY_ATTEMPTS, delay=RETRY_DELAY_SECONDS)
def _get_remote_size_du_core(ssh_client: paramiko.SSHClient, remote_path: str) -> int:
    """Core logic for getting the size of a remote path using `du -sb`.

    This is a helper function designed to be wrapped by the `@retry` decorator.
    It executes `du -sb` for a single path.

    Args:
        ssh_client: An active Paramiko `SSHClient` object.
        remote_path: The absolute path of the file or directory on the remote server.

    Returns:
        The size of the remote path in bytes.

    Raises:
        Exception: If the `du` command fails or its output cannot be parsed.
    """
    escaped_path = shlex.quote(remote_path)
    command = f"du -sb {escaped_path}"
    try:
        _stdin, stdout, stderr = ssh_client.exec_command(command, timeout=SSH_EXEC_TIMEOUT)
        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            stderr_output = stderr.read().decode('utf-8').strip()
            if "No such file or directory" in stderr_output:
                logging.warning(f"'du' command failed for path '{remote_path}': Path not found.")
                return 0
            raise Exception(f"'du' command failed with exit code {exit_status}. Stderr: {stderr_output}")

        output = stdout.read().decode('utf-8').strip()
        size_str = output.split()[0]
        if size_str.isdigit():
            return int(size_str)
        else:
            raise ValueError(f"Could not parse 'du' output. Raw output: '{output}'")
    except Exception as e:
        logging.error(f"Cannot access remote path '{remote_path}': {e}")
        raise

def batch_get_remote_sizes(ssh_client: paramiko.SSHClient, paths: typing.List[str], batch_size: int = BATCH_SIZE) -> typing.Dict[str, int]:
    """Calculates the total size of multiple remote paths efficiently in batches.

    This function uses the `du -sb` command over a single SSH execution to get
    the sizes of multiple files or directories at once. It processes paths in
    batches to avoid exceeding shell command length limits (ARG_MAX) and
    dynamically adjusts batch sizes if necessary. This is significantly faster
    than checking each path individually.

    Args:
        ssh_client: An active Paramiko `SSHClient` object.
        paths: A list of absolute paths to check on the remote server.
        batch_size: The initial number of paths to process in a single SSH command.

    Returns:
        A dictionary mapping each remote path to its size in bytes. If a path
        could not be accessed, its entry may be missing or its size may be 0.
    """
    results = {}
    if not paths:
        return results

    # Process paths in batches to avoid command length limits
    for i in range(0, len(paths), batch_size):
        batch = paths[i:i + batch_size]
        # Construct a single command string that runs `du` on each path
        command = "; ".join(f"du -sb {shlex.quote(p)} 2>/dev/null" for p in batch)

        if len(command) > MAX_COMMAND_LENGTH:
            logging.warning(f"Command for batch size calculation is too long ({len(command)} bytes). Consider reducing BATCH_SIZE.")
            # Fallback to individual processing for this oversized batch
            for path in batch:
                try:
                    results[path] = _get_remote_size_du_core(ssh_client, path)
                except Exception as e_inner:
                    logging.error(f"Individual fallback for '{path}' also failed: {e_inner}")
                    results[path] = 0
            continue

        try:
            _stdin, stdout, _stderr = ssh_client.exec_command(command, timeout=BATCH_SSH_EXEC_TIMEOUT)
            output = stdout.read().decode('utf-8').strip()

            # Parse the output, which contains one line per path
            for line in output.splitlines():
                if not line:
                    continue
                parts = line.split('\t', 1)
                if len(parts) == 2:
                    try:
                        size = int(parts[0])
                        path_from_output = parts[1]
                        # Find the original path this output corresponds to
                        original_path = next((p for p in batch if p == path_from_output), None)
                        if original_path:
                            results[original_path] = size
                        else:
                             logging.warning(f"Could not map output path '{path_from_output}' back to an original path in batch.")
                    except ValueError:
                        logging.warning(f"Could not parse size from line: {line}")
                else:
                    logging.warning(f"Unexpected output format from 'du': {line}")
        except Exception as e:
            logging.error(f"Batch size calculation failed: {e}. Falling back to individual queries for this batch.")
            for path in batch:
                try:
                    results[path] = _get_remote_size_du_core(ssh_client, path)
                except Exception as e_inner:
                    logging.error(f"Individual fallback for '{path}' also failed: {e_inner}")
                    results[path] = 0
    return results
