import paramiko
import logging
import threading
from queue import Queue, Empty
from contextlib import contextmanager
import typing
import shlex
import os
import re
import shutil
import tempfile
import getpass
import time

from utils import retry, _create_safe_command_for_logging

# Constants
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
    """Creates a user-specific directory for SSH control sockets.

    This function sets up a directory in the system's temporary folder
    to store SSH control path sockets, which enables connection multiplexing.
    This can significantly speed up subsequent SSH connections to the same host.
    The global `SSH_CONTROL_PATH` variable is set to the socket path template.
    """
    global SSH_CONTROL_PATH
    try:
        user = getpass.getuser()
        control_dir = os.path.join(tempfile.gettempdir(), f"torrent_mover_ssh_{user}")
        os.makedirs(control_dir, mode=0o700, exist_ok=True)
        SSH_CONTROL_PATH = os.path.join(control_dir, "%r@%h:%p")
        logging.debug(f"Using SSH control path: {SSH_CONTROL_PATH}")
    except Exception as e:
        logging.warning(f"Could not create SSH control path directory. Multiplexing will be disabled. Error: {e}")
        SSH_CONTROL_PATH = None

def log_secure_command(command: typing.List[str], level: int = logging.DEBUG) -> None:
    """Logs a command list securely by redacting sensitive information.

    This function utilizes the shared sanitization logic to ensure that
    sensitive arguments (like passwords in sshpass) are not written to logs.
    """
    safe_cmd = _create_safe_command_for_logging(command)
    logging.log(level, f"Executing command: {' '.join(safe_cmd)}")

def check_sshpass_installed() -> None:
    """Checks if sshpass is installed and exits if it is not.

    This function is a prerequisite check for using rsync with password
    authentication, as it relies on the `sshpass` utility to provide the
    password to the SSH command.

    Raises:
        SystemExit: If the `sshpass` command is not found in the system's PATH.
    """
    if shutil.which("sshpass") is None:
        logging.error("FATAL: 'sshpass' is not installed or not in the system's PATH.")
        logging.error("Please install 'sshpass' to use the rsync transfer mode with a password.")
        logging.error("e.g., 'sudo apt-get install sshpass' or 'sudo yum install sshpass'")
        exit(1)
    logging.debug("'sshpass' dependency check passed.")
    setup_ssh_control_path()

def _get_ssh_command(port: int) -> str:
    """Builds the SSH command for rsync, enabling connection multiplexing if available."""
    # Use aes128-gcm@openssh.com for high performance (hardware accelerated)
    # Disable compression (-o Compression=no) as it bottlenecks high-speed links
    base_ssh_cmd = f"ssh -p {port} -c aes128-gcm@openssh.com,aes128-ctr -o Compression=no -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ServerAliveInterval=15"
    if SSH_CONTROL_PATH:
        multiplex_opts = f"-o ControlMaster=auto -o ControlPath={shlex.quote(SSH_CONTROL_PATH)} -o ControlPersist=60s"
        return f"{base_ssh_cmd} {multiplex_opts}"
    return base_ssh_cmd

def sftp_mkdir_p(sftp: paramiko.SFTPClient, remote_path: str) -> None:
    """Recursively creates a directory on the SFTP server.

    This function mimics the behavior of the `mkdir -p` command, ensuring that
    the entire directory path exists on the remote server.

    Args:
        sftp: An active Paramiko SFTPClient object.
        remote_path: The absolute path of the directory to create.

    Raises:
        IOError: If a directory could not be created and does not already exist.
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
            try:
                sftp.stat(remote_path)
            except FileNotFoundError:
                raise e

def is_remote_dir(ssh_client: paramiko.SSHClient, path: str) -> bool:
    """Checks if a path on a remote server is a directory.

    This function executes the `test -d` command over SSH to efficiently
    determine if the specified path points to a directory.

    Args:
        ssh_client: An active Paramiko SSHClient object.
        path: The absolute path to check on the remote server.

    Returns:
        True if the path is a directory, False otherwise or if an error occurs.
    """
    logging.debug(f"Checking if remote path is directory: {path}")
    try:
        command = f"test -d {shlex.quote(path)}"
        stdin, stdout, stderr = ssh_client.exec_command(command, timeout=SSH_EXEC_TIMEOUT)
        exit_status = stdout.channel.recv_exit_status()
        return exit_status == 0
    except Exception as e:
        logging.error(f"Cannot access remote path: {path}\n"
                      f"This could mean:\n"
                      f" • The path doesn't exist on the remote server\n"
                      f" • You don't have permission to access it\n"
                      f" • The SSH connection was interrupted\n"
                      f"Technical details: {e}")
        return False

def _get_all_files_recursive(sftp: paramiko.SFTPClient, remote_path: str, base_dest_path: str, file_list: typing.List[typing.Tuple[str, str, int]]) -> None:
    """
    Recursively walks a remote directory to build a flat list of all files to transfer.
    `base_dest_path` is the corresponding destination path for the initial `remote_path`.
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
            if stat_info.st_mode & 0o40000:  # S_ISDIR
                _get_all_files_recursive(sftp, remote_item_path, dest_item_path, file_list)
            else:
                file_list.append((remote_item_path, dest_item_path, stat_info.st_size))
        except FileNotFoundError:
            logging.warning(f"File or directory vanished during scan: {remote_item_path}")
            continue

class SSHConnectionPool:
    """A thread-safe pool for managing Paramiko SSH and SFTP connections.

    This class provides a way to reuse SSH connections, which can significantly
    reduce the overhead of repeatedly connecting to the same server. It handles
    the creation, validation, and recycling of connections in a thread-safe manner.

    Attributes:
        host: The hostname or IP address of the SSH server.
        port: The port number of the SSH server.
        username: The username for authentication.
        password: The password for authentication.
        max_size: The maximum number of connections allowed in the pool.
        connect_timeout: The timeout in seconds for establishing a new connection.
        pool_wait_timeout: The timeout in seconds for waiting to get a connection
            from the pool when it is full.
    """

    def __init__(self, host: str, port: int, username: str, password: str,
                 max_size: int = DEFAULT_SSH_POOL_SIZE, connect_timeout: int = 10, pool_wait_timeout: int = 1200):
        """Initializes the SSHConnectionPool.

        Args:
            host: The hostname of the SSH server.
            port: The port of the SSH server.
            username: The username for authentication.
            password: The password for authentication.
            max_size: The maximum number of connections to maintain.
            connect_timeout: Timeout for establishing new connections.
            pool_wait_timeout: Timeout for waiting for a connection from the pool.
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
        self._active_connections = 0  # Track connections in use + in pool
        self._condition = threading.Condition(self._lock)  # For waiting
        self._closed = False
        logging.debug(f"Initialized SSHConnectionPool for {host} with max_size={max_size}")

    def _create_connection(self) -> typing.Tuple[paramiko.SFTPClient, paramiko.SSHClient]:
        """Create a new SSH client and SFTP session."""
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
        """Check if SSH connection is still active."""
        try:
            transport = ssh.get_transport()
            return transport is not None and transport.is_active()
        except Exception:
            return False

    @contextmanager
    def get_connection(self) -> typing.Generator[typing.Tuple[paramiko.SFTPClient, paramiko.SSHClient], None, None]:
        """Provides a connection from the pool within a context manager.

        This method handles the logic of acquiring a connection (either by
        reusing an existing one or creating a new one) and returning it to the
        pool when the context is exited. It also handles dead connections.

        Yields:
            A tuple containing an active (SFTPClient, SSHClient).

        Raises:
            RuntimeError: If the pool is closed.
            TimeoutError: If waiting for a connection exceeds `pool_wait_timeout`.
        """
        if self._closed:
            raise RuntimeError("Connection pool is closed")

        sftp, ssh = None, None
        created_new = False

        try:
            # Try to get or create a connection
            while True:
                if self._closed:
                    raise RuntimeError("Connection pool was closed while waiting")

                # Try to get existing connection from pool
                try:
                    sftp, ssh = self._pool.get_nowait()

                    # Check if it's alive
                    if self._is_connection_alive(ssh):
                        logging.debug(f"Reusing connection to {self.host}")
                        break
                    else:
                        # Dead connection, close and continue
                        logging.debug(f"Discarding dead connection to {self.host}")
                        try:
                            ssh.close()
                        except Exception:
                            pass

                        with self._lock:
                            self._active_connections -= 1
                            # Notify waiters that a slot is available
                            self._condition.notify()

                        sftp, ssh = None, None
                        continue

                except Empty:
                    # No connections available in pool
                    with self._condition:
                        if self._active_connections < self.max_size:
                            # Can create new connection
                            self._active_connections += 1
                            created_new = True
                            logging.debug(
                                f"Creating new connection to {self.host} "
                                f"({self._active_connections}/{self.max_size})"
                            )
                            break
                        else:
                            # Wait for a connection to become available
                            logging.debug(
                                f"Pool full ({self.max_size}/{self.max_size}), "
                                f"waiting for connection to {self.host}"
                            )
                            # Wait with timeout to avoid deadlock
                            if not self._condition.wait(timeout=self.pool_wait_timeout):
                                raise TimeoutError(
                                    f"Timeout waiting for SSH connection to {self.host}"
                                )

            # If we need to create a new connection, do it outside the lock
            if created_new:
                try:
                    sftp, ssh = self._create_connection()
                except Exception:
                    # Creation failed, decrement counter
                    with self._lock:
                        self._active_connections -= 1
                        self._condition.notify()
                    raise

            yield sftp, ssh

        except Exception as e:
            # Don't return broken connection to pool
            if ssh and not self._is_connection_alive(ssh):
                logging.warning(f"Connection to {self.host} found dead after use: {e}")
                with self._lock:
                    self._active_connections -= 1
                    self._condition.notify()
                try:
                    ssh.close()
                except Exception:
                    pass
            elif ssh:
                # Connection is still good but operation failed
                # Return it to pool
                try:
                    self._pool.put_nowait((sftp, ssh))
                except Exception: # Pool full, or other error
                    logging.warning(f"Could not return connection to pool (full?). Closing. {self.host}")
                    with self._lock:
                        self._active_connections -= 1
                        self._condition.notify()
                    try:
                        ssh.close()
                    except Exception:
                        pass
            raise
        else:
            # Success - return connection to pool
            if sftp and ssh:
                try:
                    self._pool.put_nowait((sftp, ssh))
                    logging.debug(f"Returned connection to pool for {self.host}. (In pool: {self._pool.qsize()})")
                except Exception:
                    # Pool is full (shouldn't happen), close connection
                    logging.warning(f"Pool was full on return. Closing connection. {self.host}")
                    with self._lock:
                        self._active_connections -= 1
                        self._condition.notify()
                    try:
                        ssh.close()
                    except Exception:
                        pass

    def close_all(self):
        """Closes all active and pooled connections.

        This method marks the pool as closed, notifies any waiting threads,
        and iterates through the queue to close each SSH client. It should be
        called during application shutdown to ensure clean resource cleanup.
        """
        logging.debug(f"Closing all connections for {self.host}...")
        self._closed = True

        # Wake up all waiting threads
        with self._condition:
            self._condition.notify_all()

        # Close all pooled connections
        while not self._pool.empty():
            try:
                sftp, ssh = self._pool.get_nowait()
                try:
                    ssh.close()
                except Exception:
                    pass
            except Empty:
                break
        logging.debug(f"Pool for {self.host} closed.")

    def get_stats(self) -> typing.Dict[str, int]:
        """Returns a dictionary with current pool statistics.

        This is useful for monitoring and debugging the connection pool's performance.

        Returns:
            A dictionary containing the number of active connections, max size,
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
    """
    Core logic for getting remote size using 'du -sb'. Does not handle concurrency.
    This function is retried on failure.
    """
    # Use shlex.quote for robust path escaping
    escaped_path = shlex.quote(remote_path)
    command = f"du -sb {escaped_path}"

    try:
        stdin, stdout, stderr = ssh_client.exec_command(command, timeout=SSH_EXEC_TIMEOUT)
        exit_status = stdout.channel.recv_exit_status()  # Wait for command to finish

        if exit_status != 0:
            stderr_output = stderr.read().decode('utf-8').strip()
            if "No such file or directory" in stderr_output:
                logging.warning(f"'du' command failed for path '{remote_path}': File not found.")
                return 0
            raise Exception(f"'du' command failed with exit code {exit_status}. Stderr: {stderr_output}")

        output = stdout.read().decode('utf-8').strip()
        # The output is like "12345\t/path/to/dir". We only need the number.
        size_str = output.split()[0]
        if size_str.isdigit():
            return int(size_str)
        else:
            raise ValueError(f"Could not parse 'du' output. Raw output: '{output}'")

    except Exception as e:
        logging.error(f"Cannot access remote file or directory: {remote_path}\n"
                      f"This could mean:\n"
                      f" • The path doesn't exist on the remote server\n"
                      f" • You don't have permission to access it\n"
                      f" • The SSH connection was interrupted\n"
                      f"Technical details: {e}")
        # Re-raise to be handled by the retry decorator or the calling function
        raise

def batch_get_remote_sizes(ssh_client: paramiko.SSHClient, paths: typing.List[str], batch_size: int = BATCH_SIZE) -> typing.Dict[str, int]:
    """Calculates the total size of multiple remote paths efficiently.

    This function uses the `du -sb` command over a single SSH session to get
    the sizes of multiple files or directories. It processes paths in batches
    to avoid exceeding shell command length limits (ARG_MAX) and dynamically
    adjusts batch sizes if necessary.

    Args:
        ssh_client: An active Paramiko SSHClient object.
        paths: A list of absolute paths to check on the remote server.
        batch_size: The number of paths to process in a single SSH command.

    Returns:
        A dictionary mapping each remote path to its size in bytes. If a path
        could not be accessed, its size is returned as 0.
    """
    results = {}

    # Dynamically adjust batch size to avoid ARG_MAX
    current_batch_size = batch_size
    estimated_length = sum(len(p) + 60 for p in paths[:current_batch_size]) # 60 chars for "du -sb '' 2>/dev/null || echo '0\t'; "

    while estimated_length > MAX_COMMAND_LENGTH and current_batch_size > 1:
        logging.warning(f"Initial batch size ({current_batch_size}) creates a command too long ({estimated_length} > {MAX_COMMAND_LENGTH}). Halving.")
        current_batch_size = max(1, current_batch_size // 2)
        estimated_length = sum(len(p) + 60 for p in paths[:current_batch_size])

    if current_batch_size == 1 and estimated_length > MAX_COMMAND_LENGTH:
        logging.error(f"A single path is too long to process: {paths[0]}. Skipping size check for it.")

    # Process in batches
    for i in range(0, len(paths), current_batch_size):
        batch = paths[i:i + current_batch_size]
        cmd_parts = []
        for path in batch:
            escaped = path.replace("'", "'\\''")
            # Output format: SIZE\tPATH
            # This ensures we get output even if 'du' fails (e.g., no permissions)
            cmd_parts.append(f"du -sb '{escaped}' 2>/dev/null")

        command = "; ".join(cmd_parts)

        if len(command) > MAX_COMMAND_LENGTH and len(batch) > 1:
             # This can happen if path lengths are very uneven
             # We'll just process this oversized batch and hope for the best
             # A more complex solution would be to build batches based on char count
             logging.warning(f"Command batch (starting with {batch[0]}) is oversized: {len(command)} bytes. Trying anyway.")
        elif len(command) > MAX_COMMAND_LENGTH:
             logging.error(f"Cannot process path, command is too long even for one item: {batch[0]}. Skipping.")
             continue

        try:
            stdin, stdout, stderr = ssh_client.exec_command(command, timeout=BATCH_SSH_EXEC_TIMEOUT)
            exit_status = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8').strip()

            # Parse output
            lines = output.splitlines()
            if len(lines) != len(batch):
                logging.warning(f"Batch size mismatch. Sent {len(batch)} commands, got {len(lines)} lines. Output: {output}")

            for line in lines:
                if not line:
                    continue
                parts = line.split('\t', 1)
                if len(parts) == 2:
                    try:
                        size = int(parts[0])
                        path_from_output = parts[1]
                        # Find the original path (case matching, etc.)
                        # This is safer than assuming the output path is identical
                        original_path = next((p for p in batch if p.endswith(path_from_output) or p == path_from_output), None)
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
                    results[path] = 0 # Mark as failed
    return results
