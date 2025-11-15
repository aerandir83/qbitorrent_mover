import time
import logging
import os
from functools import wraps
from typing import Callable, Any

def retry(tries: int = 2, delay: int = 5, backoff: int = 1) -> Callable:
    """Creates a decorator that retries a function call.

    This decorator will re-invoke the decorated function upon exceptions up to
    a specified number of times, with an optional exponential backoff.

    Args:
        tries: The maximum number of attempts.
        delay: The initial delay between retries in seconds.
        backoff: The factor by which the delay should be multiplied after each
            failed attempt. A value of 1 results in a fixed delay.

    Returns:
        A decorator that can be applied to a function.
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


class RemoteTransferError(Exception):
    """Custom exception for remote transfer failures."""
    pass


def _parse_human_readable_bytes(s: str) -> int:
    """
    Parses a human-readable size string (e.g., '49.41M', '1.2G', '1024') into bytes.
    """
    s = s.strip()
    units = {
        "k": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
    }
    # Check for a unit at the end
    unit = s[-1]
    if unit in units:
        try:
            # Value has a unit (e.g., '49.41M')
            value_str = s[:-1]
            value = float(value_str)
            return int(value * units[unit])
        except ValueError:
            # Malformed string, fall back
            return 0
    else:
        try:
            # No unit, just bytes (e.g., '1024')
            return int(s.replace(',', ''))
        except ValueError:
            return 0

def _create_safe_command_for_logging(command: list[str]) -> list[str]:
    """Creates a copy of a command list with the sshpass password redacted."""
    safe_command = list(command)
    try:
        # Find the index of 'sshpass' and redact the password after the '-p' flag
        sshpass_index = safe_command.index("sshpass")
        if "-p" in safe_command[sshpass_index:]:
            p_index = safe_command.index("-p", sshpass_index)
            if p_index + 1 < len(safe_command):
                safe_command[p_index + 1] = "'********'"
    except ValueError:
        # 'sshpass' not in command, nothing to redact
        pass
    return safe_command

class RateLimitedFile:
    """Wraps a file-like object to throttle read and write operations.

    This class acts as a proxy to a file object, inserting delays into read()
    and write() calls to ensure that the data transfer rate does not exceed a
    specified maximum.

    Attributes:
        file: The underlying file-like object (e.g., from `open()` or SFTP).
        max_bytes_per_sec: The maximum desired transfer speed in bytes per second.
    """
    def __init__(self, file_obj: Any, max_bytes_per_sec: float):
        """Initializes the RateLimitedFile wrapper.

        Args:
            file_obj: The file-like object to wrap.
            max_bytes_per_sec: The maximum transfer speed in bytes per second.
                If 0 or None, the limit is infinite (no throttling).
        """
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

class Timeouts:
    SSH_CONNECT = int(os.getenv('TM_SSH_CONNECT_TIMEOUT', '10'))
    SSH_EXEC = int(os.getenv('TM_SSH_EXEC_TIMEOUT', '60'))
    SFTP_TRANSFER = int(os.getenv('TM_SFTP_TIMEOUT', '300'))
    RECHECK = int(os.getenv('TM_RECHECK_TIMEOUT', '900'))
    POOL_WAIT = int(os.getenv('TM_POOL_WAIT_TIMEOUT', '120'))
