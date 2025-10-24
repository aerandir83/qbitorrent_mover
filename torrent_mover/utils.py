import time
import logging
from functools import wraps
import paramiko
import logging
import threading
from queue import Queue, Empty
from contextlib import contextmanager

class SSHConnectionPool:
    """Thread-safe SSH connection pool to reuse connections."""

    def __init__(self, host, port, username, password, max_size=5, timeout=30):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.max_size = max_size
        self.timeout = timeout
        self._pool = Queue(maxsize=max_size)
        self._lock = threading.Lock()
        self._created = 0

    def _create_connection(self):
        """Create a new SSH client and SFTP session."""
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
            transport.set_keepalive(30)
        sftp = ssh_client.open_sftp()
        return sftp, ssh_client

    @contextmanager
    def get_connection(self):
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
                        sftp, ssh = self._pool.get(timeout=60)

            yield sftp, ssh

        except Exception as e:
            # Don't return broken connection to pool
            if ssh:
                try:
                    ssh.close()
                except:
                    pass
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

import os
import fcntl
import atexit
from pathlib import Path

class LockFile:
    """Atomic lock file using fcntl (Unix only)."""

    def __init__(self, lock_path):
        self.lock_path = Path(lock_path)
        self.lock_fd = None
        self._acquired = False

    def acquire(self):
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

    def release(self):
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

    def get_locking_pid(self):
        """Read the PID from the lock file, if it exists."""
        if self.lock_path.exists():
            try:
                return self.lock_path.read_text().strip()
            except IOError:
                return None
        return None


def retry(tries=2, delay=5, backoff=1):
    """
    A decorator for retrying a function call with a specified delay.

    :param tries: The maximum number of attempts.
    :param delay: The delay between retries in seconds.
    :param backoff: The factor by which the delay should grow (default is 1 for fixed delay).
    """
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
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