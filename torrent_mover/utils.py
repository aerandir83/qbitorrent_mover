import time
import logging
from functools import wraps
import paramiko
import logging
import threading
from queue import Queue, Empty
from contextlib import contextmanager
from typing import Optional, Dict, List, Tuple, Any, Callable
from pathlib import Path
import os
import fcntl
import atexit
import configparser
import sys
import shutil
import json

# Constants
DEFAULT_SSH_TIMEOUT = 30
DEFAULT_KEEPALIVE_INTERVAL = 30
DEFAULT_SSH_POOL_SIZE = 5
DEFAULT_RETRY_ATTEMPTS = 2
DEFAULT_RETRY_DELAY = 5
SSH_EXEC_TIMEOUT = 60
BATCH_SSH_EXEC_TIMEOUT = 120
BATCH_SIZE = 10


class SSHConnectionPool:
    """Thread-safe SSH connection pool to reuse connections."""

    def __init__(self, host: str, port: int, username: str, password: str, max_size: int = DEFAULT_SSH_POOL_SIZE, timeout: int = DEFAULT_SSH_TIMEOUT):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.max_size = max_size
        self.timeout = timeout
        self._pool: Queue[Tuple[paramiko.SFTPClient, paramiko.SSHClient]] = Queue(maxsize=max_size)
        self._lock = threading.Lock()
        self._created = 0

    def _create_connection(self) -> Tuple[paramiko.SFTPClient, paramiko.SSHClient]:
        """Create a new SSH client and SFTP session."""
        try:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                timeout=self.timeout
            )
            transport = ssh_client.get_transport()
            if transport:
                transport.set_keepalive(DEFAULT_KEEPALIVE_INTERVAL)
            sftp = ssh_client.open_sftp()
            return sftp, ssh_client
        except Exception as e:
            logging.error(f"Failed to create SSH connection to {self.host}:{self.port}: {e}")
            raise

    @contextmanager
    def get_connection(self) -> Tuple[paramiko.SFTPClient, paramiko.SSHClient]:
        """Context manager to get a connection from the pool."""
        sftp, ssh = None, None
        try:
            # Try to get existing connection
            try:
                sftp, ssh = self._pool.get_nowait()
                # Verify connection is still alive
                transport = ssh.get_transport()
                if not transport or not transport.is_active():
                    # Connection is dead, close it and decrement the counter
                    with self._lock:
                        self._created -= 1
                    try:
                        ssh.close()
                    except:
                        pass
                    raise AttributeError("Connection is dead")
            except (Empty, AttributeError):
                # Create new connection if pool is empty or connection is dead
                with self._lock:
                    if self._created < self.max_size:
                        sftp, ssh = self._create_connection()
                        self._created += 1
                        logging.debug(f"Created new connection ({self._created}/{self.max_size})")
                    else:
                        # Wait for a connection to become available
                        sftp, ssh = self._pool.get(timeout=self.timeout)

            yield sftp, ssh

        except Exception as e:
            # Don't return broken connection to pool
            if ssh:
                try:
                    ssh.close()
                except:
                    pass
            logging.error(f"Error with SSH connection: {e}")
            raise
        else:
            # Return connection to pool
            try:
                self._pool.put_nowait((sftp, ssh))
            except:
                # Pool is full, close connection
                if ssh:
                    ssh.close()

    def close_all(self):
        """Close all connections in the pool."""
        while not self._pool.empty():
            try:
                sftp, ssh = self._pool.get_nowait()
                if ssh:
                    ssh.close()
            except Empty:
                break


class LockFile:
    """Atomic lock file using fcntl (Unix only)."""

    def __init__(self, lock_path: Path):
        self.lock_path = lock_path
        self.lock_fd: Optional[int] = None
        self._acquired = False

    def acquire(self) -> None:
        """Acquire the lock. Raises RuntimeError if already locked."""
        try:
            # Open the file, creating it if it doesn't exist
            self.lock_fd = open(self.lock_path, 'w')
            # Try to acquire an exclusive, non-blocking lock
            fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Write the current PID to the lock file
            self.lock_fd.write(str(os.getpid()))
            self.lock_fd.flush()
            # Register the release method to be called on exit
            atexit.register(self.release)
            self._acquired = True
        except (IOError, BlockingIOError):
            # If locking fails, close the file descriptor if it was opened
            if self.lock_fd:
                self.lock_fd.close()
            # Announce that the script is already running
            pid = self.get_locking_pid()
            if pid:
                raise RuntimeError(f"Script is already running with PID {pid} (lock file: {self.lock_path})")
            else:
                raise RuntimeError(f"Script is already running (lock file: {self.lock_path})")

    def release(self) -> None:
        """Release the lock."""
        if self.lock_fd and self._acquired:
            try:
                # Release the lock
                fcntl.flock(self.lock_fd.fileno(), fcntl.LOCK_UN)
                self.lock_fd.close()
                self.lock_path.unlink(missing_ok=True)
                self._acquired = False
            except Exception as e:
                logging.error(f"Error releasing lock file '{self.lock_path}': {e}")
            finally:
                self.lock_fd = None

    def get_locking_pid(self) -> Optional[str]:
        """Read the PID from the lock file, if it exists."""
        if self.lock_path.exists():
            try:
                return self.lock_path.read_text().strip()
            except IOError:
                return None
        return None


def retry(tries: int = DEFAULT_RETRY_ATTEMPTS, delay: int = DEFAULT_RETRY_DELAY, backoff: int = 1) -> Callable:
    """
    A decorator for retrying a function call with a specified delay.

    :param tries: The maximum number of attempts.
    :param delay: The delay between retries in seconds.
    :param backoff: The factor by which the delay should grow (default is 1 for fixed delay).
    """
    def deco_retry(f: Callable) -> Callable:
        @wraps(f)
        def f_retry(*args: Any, **kwargs: Any) -> Any:
            _tries, _delay = tries, delay
            for attempt in range(1, _tries + 1):
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    if attempt == _tries:
                        logging.error(f"'{f.__name__}' failed on the final attempt ({attempt}/{_tries}): {e}")
                        raise

                    msg = (f"'{f.__name__}' failed with '{e}'. Attempt {attempt}/{_tries}. "
                           f"Retrying in {_delay} seconds...")
                    logging.warning(msg)
                    time.sleep(_delay)
                    _delay *= backoff
        return f_retry
    return deco_retry

@retry(tries=DEFAULT_RETRY_ATTEMPTS, delay=DEFAULT_RETRY_DELAY)
def _get_remote_size_du_core(ssh_client: paramiko.SSHClient, remote_path: str) -> int:
    """
    Core logic for getting remote size using 'du -sb'. Does not handle concurrency.
    This function is retried on failure.
    """
    # Escape single quotes in the path to prevent command injection issues.
    escaped_path = remote_path.replace("'", "'\\''")
    command = f"du -sb '{escaped_path}'"

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

def batch_get_remote_sizes(ssh_client: paramiko.SSHClient, paths: List[str], batch_size: int = BATCH_SIZE) -> Dict[str, int]:
    """
    Get sizes for multiple paths in a single SSH session.
    Uses a single command with multiple du calls.
    """
    results = {}
    # Process in batches to avoid command length limits
    for i in range(0, len(paths), batch_size):
        batch = paths[i:i + batch_size]
        # Build command that gets all sizes in one go
        # Using printf to get parseable output
        cmd_parts = []
        for path in batch:
            escaped = path.replace("'", "'\\''")
            # Output format: SIZE\tPATH
            cmd_parts.append(f"du -sb '{escaped}' 2>/dev/null || echo '0\t{escaped}'")
        # Join with semicolons
        command = "; ".join(cmd_parts)
        try:
            stdin, stdout, stderr = ssh_client.exec_command(command, timeout=BATCH_SSH_EXEC_TIMEOUT)
            exit_status = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8').strip()
            # Parse output
            for line in output.split('\n'):
                if not line:
                    continue
                parts = line.split('\t', 1)
                if len(parts) == 2:
                    try:
                        size = int(parts[0])
                        path = parts[1]
                        results[path] = size
                    except ValueError:
                        logging.warning(f"Could not parse size from line: {line}")
        except Exception as e:
            logging.error(f"Batch size calculation failed: {e}")
            # Fall back to individual queries
            for path in batch:
                try:
                    results[path] = _get_remote_size_du_core(ssh_client, path)
                except:
                    results[path] = 0
    return results


class ConfigValidator:
    """Validates configuration file structure and values."""

    REQUIRED_SECTIONS = {
        'SOURCE_CLIENT': ['host', 'port', 'username', 'password'],
        'DESTINATION_CLIENT': ['host', 'port', 'username', 'password'],
        'SOURCE_SERVER': ['host', 'port', 'username', 'password'],
        'DESTINATION_PATHS': ['destination_path'],
        'SETTINGS': ['source_client_section', 'destination_client_section',
                     'category_to_move', 'transfer_mode']
    }

    VALID_TRANSFER_MODES = ['sftp', 'rsync', 'sftp_upload']

    def __init__(self, config: configparser.ConfigParser):
        self.config = config
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def validate(self) -> bool:
        """Run all validation checks. Returns True if config is valid."""
        self._check_required_sections()
        self._check_required_options()
        self._check_transfer_mode()
        self._check_paths()
        self._check_numeric_values()
        self._check_client_sections_exist()

        if self.errors:
            print("Configuration errors found:", file=sys.stderr)
            for error in self.errors:
                print(f" ❌ {error}", file=sys.stderr)
            return False

        if self.warnings:
            print("Configuration warnings:", file=sys.stderr)
            for warning in self.warnings:
                print(f" ⚠️ {warning}", file=sys.stderr)

        return True

    def _check_required_sections(self) -> None:
        """Ensure required sections exist."""
        for section in self.REQUIRED_SECTIONS:
            if not self.config.has_section(section):
                self.errors.append(f"Missing required section: [{section}]")

    def _check_required_options(self) -> None:
        """Ensure required options exist in each section."""
        for section, options in self.REQUIRED_SECTIONS.items():
            if not self.config.has_section(section):
                continue
            for option in options:
                if not self.config.has_option(section, option):
                    self.errors.append(f"Missing option '{option}' in [{section}]")
                elif not self.config.get(section, option).strip():
                    self.errors.append(f"Option '{option}' in [{section}] is empty")

    def _check_transfer_mode(self) -> None:
        """Validate transfer mode and related requirements."""
        if not self.config.has_section('SETTINGS'):
            return

        mode = self.config.get('SETTINGS', 'transfer_mode', fallback='sftp').lower()
        if mode not in self.VALID_TRANSFER_MODES:
            self.errors.append(f"Invalid transfer_mode '{mode}'. Must be one of: {', '.join(self.VALID_TRANSFER_MODES)}")

        # Check mode-specific requirements
        if mode == 'sftp_upload' and not self.config.has_section('DESTINATION_SERVER'):
            self.errors.append("transfer_mode='sftp_upload' requires [DESTINATION_SERVER] section")

        if mode == 'rsync' and not shutil.which('rsync'):
            self.warnings.append("rsync mode selected but 'rsync' command not found in PATH")

    def _check_paths(self) -> None:
        """Validate path configurations."""
        if not self.config.has_section('DESTINATION_PATHS'):
            return

        dest_path = self.config.get('DESTINATION_PATHS', 'destination_path', fallback='')
        if dest_path and not Path(dest_path).is_absolute():
            self.warnings.append(f"destination_path '{dest_path}' is not an absolute path")

    def _check_numeric_values(self) -> None:
        """Validate numeric configuration values."""
        if not self.config.has_section('SETTINGS'):
            return

        numeric_options = {
            'max_concurrent_downloads': (1, 20),
            'max_concurrent_uploads': (1, 20),
        }

        for option, (min_val, max_val) in numeric_options.items():
            if self.config.has_option('SETTINGS', option):
                try:
                    value = self.config.getint('SETTINGS', option)
                    if value < min_val or value > max_val:
                        self.warnings.append(
                            f"{option}={value} is outside recommended range [{min_val}-{max_val}]"
                        )
                except ValueError:
                    self.errors.append(f"Option '{option}' must be an integer")

    def _check_client_sections_exist(self) -> None:
        """Verify that referenced client sections exist."""
        if not self.config.has_section('SETTINGS'):
            return

        source_section = self.config.get('SETTINGS', 'source_client_section', fallback='')
        dest_section = self.config.get('SETTINGS', 'destination_client_section', fallback='')

        if source_section and not self.config.has_section(source_section):
            self.errors.append(f"source_client_section references non-existent section [{source_section}]")

        if dest_section and not self.config.has_section(dest_section):
            self.errors.append(f"destination_client_section references non-existent section [{dest_section}]")

class RateLimitedFile:
    """Wrapper for file objects that limits read/write speed."""
    def __init__(self, file_obj: Any, max_bytes_per_sec: float):
        self.file = file_obj
        self.max_bytes_per_sec = max_bytes_per_sec if max_bytes_per_sec else float('inf')
        self.last_time = time.time()
        self.bytes_since_last = 0

    def read(self, size: int) -> bytes:
        data = self.file.read(size)
        self._throttle(len(data))
        return data

    def write(self, data: bytes) -> int:
        bytes_written = self.file.write(data)
        if bytes_written:
            self._throttle(bytes_written)
        return bytes_written

    def _throttle(self, bytes_transferred: int) -> None:
        if self.max_bytes_per_sec == float('inf'):
            return

        self.bytes_since_last += bytes_transferred
        elapsed = time.time() - self.last_time

        if elapsed > 0:
            required_time = self.bytes_since_last / self.max_bytes_per_sec
            sleep_time = required_time - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

        if elapsed >= 1.0:
            self.last_time = time.time()
            self.bytes_since_last = 0

    def __getattr__(self, attr: str) -> Any:
        """Proxy other attributes to the wrapped file object."""
        return getattr(self.file, attr)


class TransferCheckpoint:
    def __init__(self, checkpoint_file: Path):
        self.file = checkpoint_file
        self.state = self._load()

    def _load(self) -> Dict[str, List[str]]:
        if self.file.exists():
            try:
                return json.loads(self.file.read_text())
            except json.JSONDecodeError:
                logging.warning(f"Could not decode checkpoint file '{self.file}'. Starting fresh.")
                return {"completed": []}
        return {"completed": []}

    def mark_completed(self, torrent_hash: str) -> None:
        if torrent_hash not in self.state["completed"]:
            self.state["completed"].append(torrent_hash)
            self._save()

    def is_completed(self, torrent_hash: str) -> bool:
        return torrent_hash in self.state["completed"]

    def _save(self) -> None:
        try:
            self.file.write_text(json.dumps(self.state, indent=2))
        except IOError as e:
            logging.error(f"Failed to save checkpoint file '{self.file}': {e}")