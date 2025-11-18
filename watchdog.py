import threading
import time
import logging
import os

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ui import BaseUIManager


class TransferWatchdog:
    """
    Monitors transfer progress and force-exits the script if it gets stuck.
    """

    def __init__(self, timeout_seconds: int):
        self._timeout_seconds = timeout_seconds
        self._ui_manager: "BaseUIManager" = None
        self._watchdog_thread = None
        self._stop_event = threading.Event()

    def start(self, ui_manager: "BaseUIManager"):
        self._ui_manager = ui_manager
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, daemon=True)
        self._watchdog_thread.start()
        logging.info(f"Transfer watchdog started with a {self._timeout_seconds}s timeout.")

    def _watchdog_loop(self):
        last_progress = -1
        # Use time.monotonic() for consistent time measurement
        last_activity_time = time.monotonic()

        while not self._stop_event.is_set():
            time.sleep(30)
            if self._stop_event.is_set():
                break

            if self._ui_manager and hasattr(self._ui_manager, "_stats"):
                current_progress = self._ui_manager._stats.get("transferred_bytes", 0)
                # Get the last activity timestamp from the UI
                last_ui_activity = self._ui_manager._stats.get("last_activity_timestamp", 0)

                if current_progress > last_progress:
                    # Byte progress was made
                    last_progress = current_progress
                    last_activity_time = time.monotonic()
                    # Also update the UI stat in case pet wasn't called
                    self._ui_manager.pet_watchdog()
                elif last_ui_activity > last_activity_time:
                    # No byte progress, but other activity (e.g., recheck) was registered
                    last_activity_time = last_ui_activity
                else:
                    # No byte progress and no other activity
                    elapsed_time = time.monotonic() - last_activity_time
                    if elapsed_time > self._timeout_seconds:
                        logging.error(
                            f"No transfer progress or other activity for {self._timeout_seconds} seconds. "
                            "Watchdog is terminating the process."
                        )
                        # Use os._exit(1) for an immediate, hard exit in a daemon thread
                        os._exit(1)

    def stop(self):
        self._stop_event.set()
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=5)
        logging.info("Transfer watchdog stopped.")
