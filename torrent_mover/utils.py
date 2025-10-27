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
import socket

# Constants
DEFAULT_KEEPALIVE_INTERVAL = 30
DEFAULT_SSH_POOL_SIZE = 5
DEFAULT_RETRY_ATTEMPTS = 2
DEFAULT_RETRY_DELAY = 5
SSH_EXEC_TIMEOUT = 60
BATCH_SSH_EXEC_TIMEOUT = 120
BATCH_SIZE = 10
MAX_COMMAND_LENGTH = 100000  # Conservative limit for shell command length


class SSHConnectionPool:
    """Thread-safe SSH connection pool with proper dead connection handling."""

    def __init__(self, host: str, port: int, username: str, password: str,
                 max_size: int = DEFAULT_SSH_POOL_SIZE, connect_timeout: int = 10, pool_wait_timeout: int = 120):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.max_size = max_size
        self.connect_timeout = connect_timeout
        self.pool_wait_timeout = pool_wait_timeout
        self._pool: Queue[Tuple[paramiko.SFTPClient, paramiko.SSHClient]] = Queue(maxsize=max_size)
        self._lock = threading.Lock()
        self._active_connections = 0  # Track connections in use + in pool
        self._condition = threading.Condition(self._lock)  # For waiting
        self._closed = False
        logging.debug(f"Initialized SSHConnectionPool for {host} with max_size={max_size}")

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
    def get_connection(self) -> Tuple[paramiko.SFTPClient, paramiko.SSHClient]:
        """Context manager to get a connection from the pool."""
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
        """Close all connections in the pool."""
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

    def get_stats(self) -> Dict[str, int]:
        """Get pool statistics."""
        with self._lock:
            in_pool = self._pool.qsize()
            in_use = self._active_connections - in_pool
            return {
                "active_connections": self._active_connections,
                "max_size": self.max_size,
                "in_pool": in_pool,
                "in_use": in_use
            }


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
    Uses a single command with multiple du calls, respecting command length limits.
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
            cmd_parts.append(f"du -sb '{escaped}' 2>/dev/null || echo '0\t{escaped}'")

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


class FileTransferTracker:
    """Track partial file transfers for resume capability."""

    def __init__(self, checkpoint_file: Path):
        self.file = checkpoint_file
        self.state = self._load()

    def _load(self) -> Dict[str, Any]:
        if self.file.exists():
            try:
                return json.loads(self.file.read_text())
            except json.JSONDecodeError:
                logging.warning(f"Could not decode file transfer tracker file '{self.file}'. Starting fresh.")
                return {"files": {}}
        return {"files": {}}

    def record_file_progress(self, torrent_hash: str, file_path: str, bytes_transferred: int) -> None:
        """Record progress for a specific file."""
        key = f"{torrent_hash}:{file_path}"
        self.state["files"][key] = {
            "bytes": bytes_transferred,
            "timestamp": time.time()
        }
        self._save()

    def get_file_progress(self, torrent_hash: str, file_path: str) -> int:
        """Get last known progress for a file."""
        key = f"{torrent_hash}:{file_path}"
        return self.state["files"].get(key, {}).get("bytes", 0)

    def _save(self) -> None:
        try:
            # Throttle saves to once every 5 seconds to reduce I/O
            if not hasattr(self, "_last_save_time"):
                 self._last_save_time = 0 # Initialize if it doesn't exist

            if time.time() - self._last_save_time > 5:
                self.file.write_text(json.dumps(self.state, indent=2))
                self._last_save_time = time.time()
        except IOError as e:
            logging.error(f"Failed to save file transfer tracker file '{self.file}': {e}")