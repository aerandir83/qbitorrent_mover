"""Provides shared utility functions and decorators.

This module currently contains a general-purpose `@retry` decorator for handling
transient errors in function calls.
"""
import time
import logging
from functools import wraps
from typing import Callable, Any

def retry(tries: int = 2, delay: int = 5, backoff: int = 1) -> Callable:
    """A decorator for retrying a function with a specified delay.

    This decorator wraps a function and will re-execute it if it raises an
    exception, up to a specified number of `tries`.

    Args:
        tries: The maximum number of attempts.
        delay: The initial delay between retries in seconds.
        backoff: A multiplier for the delay. For example, a backoff of 2 will
                 double the delay after each failed attempt. A value of 1
                 results in a fixed delay.

    Returns:
        The wrapped function.
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
