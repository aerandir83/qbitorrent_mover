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
import json
import socket
import shlex

# Constants
DEFAULT_RETRY_ATTEMPTS = 2
DEFAULT_RETRY_DELAY = 5

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
