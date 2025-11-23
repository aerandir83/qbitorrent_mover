import time
import logging
import os
from functools import wraps
from typing import Callable, Any, List

class Timeouts:
    SSH_CONNECT = int(os.getenv('TM_SSH_CONNECT_TIMEOUT', '10'))
    SSH_EXEC = int(os.getenv('TM_SSH_EXEC_TIMEOUT', '60'))
    SFTP_TRANSFER = int(os.getenv('TM_SFTP_TIMEOUT', '300'))
    RECHECK = int(os.getenv('TM_RECHECK_TIMEOUT', '900'))
    POOL_WAIT = int(os.getenv('TM_POOL_WAIT_TIMEOUT', '120'))

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

def _create_safe_command_for_logging(command: List[str]) -> List[str]:
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
