"""Provides utility functions and custom exceptions for the application.

This module contains common helper utilities that are used across various parts
of the torrent_mover package.

Classes:
    RemoteTransferError: A custom exception for handling remote transfer failures.

Functions:
    retry: A decorator that retries a function call upon failure with
           configurable delay and backoff.
"""
import time
import logging
from functools import wraps
from typing import Callable, Any, TypeVar

# A generic TypeVar to preserve function signatures in the decorator
F = TypeVar('F', bound=Callable[..., Any])

def retry(tries: int = 2, delay: int = 5, backoff: int = 1) -> Callable[[F], F]:
    """Creates a decorator that retries a function upon failure.

    This decorator will re-invoke the decorated function if it raises an exception.
    It supports a configurable number of retries, an initial delay, and an
    exponential backoff factor.

    Args:
        tries: The maximum number of attempts to make.
        delay: The initial delay between retries in seconds.
        backoff: The factor by which the delay is multiplied after each failed
            attempt. A value of 1 results in a fixed delay.

    Returns:
        A decorator that can be applied to a function to make it resilient to
        transient failures.
    """
    def deco_retry(f: F) -> F:
        @wraps(f)
        def f_retry(*args: Any, **kwargs: Any) -> Any:
            _tries, _delay = tries, delay
            for attempt in range(1, _tries + 1):
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    if attempt == _tries:
                        logging.error(f"Function '{f.__name__}' failed on the final attempt ({attempt}/{_tries}): {e}")
                        raise

                    msg = (f"Function '{f.__name__}' failed with '{e}'. Attempt {attempt}/{_tries}. "
                           f"Retrying in {_delay} seconds...")
                    logging.warning(msg)
                    time.sleep(_delay)
                    _delay *= backoff
            # This line should not be reachable, but is here for type safety.
            # The loop will either return a result or raise an exception.
            raise RuntimeError("Exited retry loop unexpectedly.")
        return f_retry  # type: ignore
    return deco_retry


class RemoteTransferError(Exception):
    """Custom exception raised for critical, non-transient remote transfer failures.

    This exception is used to signal that a transfer (e.g., rsync or SFTP) has
    failed in a way that is unlikely to be resolved by a simple retry, such as
    a permission error, allowing for specific error handling higher up in the
    call stack.
    """
    pass
