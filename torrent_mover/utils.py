import time
import logging
from functools import wraps

def retry(tries=3, delay=5, backoff=2):
    """
    A decorator for retrying a function call with exponential backoff.

    :param tries: The maximum number of attempts.
    :param delay: The initial delay between retries in seconds.
    :param backoff: The factor by which the delay should grow after each retry.
    """
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    msg = f"'{f.__name__}' failed with '{e}', retrying in {mdelay} seconds..."
                    logging.warning(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            # Last attempt
            return f(*args, **kwargs)
        return f_retry
    return deco_retry