"""Monitors transfer progress and terminates the script if it stalls.

This module provides a `TransferWatchdog` class that runs in a background
thread. It periodically checks if the total transferred bytes has increased. If
no progress is made for a configurable timeout period, it assumes the process
is stuck and force-exits the application to prevent indefinite hangs,
particularly in non-interactive environments.
"""
import threading
import time
import logging
import os
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from torrent_mover.ui import BaseUIManager


class TransferWatchdog:
    """Monitors transfer progress and force-exits the script if it gets stuck.

    This class runs a background thread that periodically checks the total bytes
    transferred via the UI manager. If the progress remains unchanged for a
    specified timeout period, it logs a fatal error and terminates the script
    with a non-zero exit code.

    This is a safeguard against stalls or deadlocks in the transfer process,
    ensuring that the script does not hang indefinitely.
    """

    def __init__(self, timeout_seconds: int):
        """Initializes the TransferWatchdog.

        Args:
            timeout_seconds: The number of seconds without any transfer progress
                before the watchdog terminates the script.
        """
        self._timeout_seconds = timeout_seconds
        self._ui_manager: Optional["BaseUIManager"] = None
        self._watchdog_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self, ui_manager: "BaseUIManager") -> None:
        """Starts the watchdog in a background thread.

        Args:
            ui_manager: The UI manager instance, used to access progress statistics.
                It must have a `_stats` attribute containing `transferred_bytes`.
        """
        self._ui_manager = ui_manager
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)
        self._watchdog_thread.start()
        logging.info(f"Transfer watchdog started with a {self._timeout_seconds}s timeout.")

    def _watchdog_loop(self) -> None:
        """The main loop for the watchdog thread.

        This method runs until the `stop` event is set. It periodically wakes up,
        checks for progress, and exits if a stall is detected.
        """
        last_progress = -1
        last_progress_time = time.monotonic()

        while not self._stop_event.wait(30):  # Check every 30 seconds
            if self._ui_manager and hasattr(self._ui_manager, "_stats"):
                # Type ignore because _stats is a protected member of the UI manager
                current_progress = self._ui_manager._stats.get("transferred_bytes", 0)  # type: ignore

                if current_progress > last_progress:
                    # Progress has been made, reset the timer
                    last_progress = current_progress
                    last_progress_time = time.monotonic()
                else:
                    # No progress, check if the timeout has been exceeded
                    elapsed_time = time.monotonic() - last_progress_time
                    if elapsed_time > self._timeout_seconds:
                        logging.error(
                            f"Watchdog Timeout: No transfer progress has been made for over {self._timeout_seconds} seconds."
                        )
                        logging.error("The script appears to be stuck. Forcibly terminating process.")
                        os._exit(1)  # Force exit, as the main threads are likely deadlocked

    def stop(self) -> None:
        """Signals the watchdog thread to stop and waits for it to terminate."""
        self._stop_event.set()
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=5)
        logging.info("Transfer watchdog stopped.")
