import logging
import threading
from time import time, sleep
import os
from collections import deque
from typing import Callable, Deque, List, Optional, Dict, Any

logger = logging.getLogger(__name__)

class SpeedMonitor(threading.Thread):
    """
    A thread-based class to monitor file size changes and calculate transfer speed.
    """

    def __init__(
        self,
        file_path: str,
        size_fetcher: Optional[Callable[[], int]] = None,
        interval: float = 1.0
    ):
        """
        Initialize the SpeedMonitor.

        Args:
            file_path: The path to the file to monitor.
            size_fetcher: A callable that returns the file size in bytes.
                          Defaults to os.path.getsize(file_path).
            interval: The polling interval in seconds.
        """
        super().__init__(name="SpeedMonitorThread")
        self.file_path = file_path

        if size_fetcher:
            self.size_fetcher = size_fetcher
        else:
            self.size_fetcher = lambda: os.path.getsize(self.file_path)

        self.interval = interval

        # State
        self._history: Deque[float] = deque(maxlen=300)
        self._smooth_window: Deque[float] = deque(maxlen=30)
        self._running = False
        self._lock = threading.Lock()

        # Daemon thread so it doesn't block program exit
        self.daemon = True

    def start_monitoring(self) -> None:
        """Starts the background monitoring thread."""
        if not self.is_alive():
            self._running = True
            self.start()
            logger.info(f"Speed monitoring started for {self.file_path}")

    def stop_monitoring(self) -> None:
        """Stops the background monitoring thread safely."""
        self._running = False
        logger.info("Speed monitoring stop requested.")

    def run(self) -> None:
        """Override Thread.run to execute _run."""
        self._run()

    def _run(self) -> None:
        """
        The main loop.
        Sleeps for interval.
        Calls size_fetcher().
        Calculates delta: (current_size - last_size) / (current_time - last_time).
        Handling Resets: If delta < 0 (file replaced/reset), ignore this sample and reset last_size.
        Updates _history and _smooth_window.
        """
        # Initialize last_size and last_time
        # We grab the initial state before the loop so we have a baseline.
        try:
            last_size = self.size_fetcher()
        except FileNotFoundError:
            last_size = 0
        except Exception as e:
            logger.error(f"Error fetching initial size: {e}")
            last_size = 0

        last_time = time()

        while self._running:
            sleep(self.interval)

            if not self._running:
                break

            try:
                current_size = self.size_fetcher()
                current_time = time()

                delta_size = current_size - last_size
                delta_time = current_time - last_time

                if delta_time <= 0:
                    # Should not happen with sleep, but prevent division by zero
                    continue

                if delta_size < 0:
                    # File reset or replaced
                    logger.debug("File size decreased (reset detected). Ignoring sample.")
                    last_size = current_size
                    last_time = current_time
                    continue

                speed = delta_size / delta_time

                with self._lock:
                    self._history.append(speed)
                    self._smooth_window.append(speed)

                last_size = current_size
                last_time = current_time

            except FileNotFoundError:
                # File might be temporarily unavailable or starting up
                logger.debug("File not found. Reporting speed 0.")
                with self._lock:
                    self._history.append(0.0)
                    self._smooth_window.append(0.0)
                # If file is gone, treat current size as 0 for next comparison?
                last_size = 0
                last_time = time()

            except Exception as e:
                logger.error(f"Error in speed monitoring loop: {e}")
                # Keep thread alive, report 0 speed
                with self._lock:
                    self._history.append(0.0)
                    self._smooth_window.append(0.0)
                last_time = time()

    def get_status(self) -> Dict[str, Any]:
        """
        Returns a thread-safe dict:
        current_speed: (float) Average of _smooth_window.
        history: (list) Copy of _history (for Sparkline).
        """
        with self._lock:
            if not self._smooth_window:
                current_speed = 0.0
            else:
                current_speed = sum(self._smooth_window) / len(self._smooth_window)

            history_copy = list(self._history)

        return {
            "current_speed": current_speed,
            "history": history_copy
        }
