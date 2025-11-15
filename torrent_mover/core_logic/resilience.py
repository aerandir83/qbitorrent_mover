import os
import logging
from collections import deque
from enum import Enum
import time
import threading
from typing import Optional, Callable, Any, Dict, Tuple, List
from dataclasses import dataclass

@dataclass
class TransferFile:
    """Normalized file representation across all transfer modes."""
    source_path: str
    dest_path: str
    size: int
    torrent_hash: str

    @property
    def normalized_source(self) -> str:
        """Returns source path with quotes stripped."""
        return self.source_path.strip('\'"')

    @property
    def normalized_dest(self) -> str:
        """Returns dest path with quotes stripped."""
        return self.dest_path.strip('\'"')

class ConnectionState(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"  # Some failures, but still trying
    CIRCUIT_OPEN = "circuit_open"  # Too many failures, stop trying

class CircuitBreaker:
    """Prevents cascading failures by stopping attempts to failing servers."""

    def __init__(self, failure_threshold: int = 5, timeout_seconds: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time = 0
        self.state = ConnectionState.HEALTHY
        self._lock = threading.Lock()

    def record_success(self):
        with self._lock:
            self.failure_count = max(0, self.failure_count - 1)
            if self.failure_count == 0:
                self.state = ConnectionState.HEALTHY

    def record_failure(self) -> ConnectionState:
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = ConnectionState.CIRCUIT_OPEN
                logging.warning(f"Circuit breaker opened after {self.failure_count} failures")
            elif self.failure_count >= self.failure_threshold // 2:
                self.state = ConnectionState.DEGRADED

            return self.state

    def can_attempt(self) -> bool:
        with self._lock:
            if self.state != ConnectionState.CIRCUIT_OPEN:
                return True

            # Check if timeout has elapsed
            if time.time() - self.last_failure_time > self.timeout_seconds:
                logging.info("Circuit breaker timeout elapsed, attempting half-open state")
                self.state = ConnectionState.DEGRADED
                self.failure_count = self.failure_threshold // 2
                return True

            return False

class ResilientTransferQueue:
    """Queue that retries failed transfers with exponential backoff."""

    def __init__(self, max_retries: int = 5):
        self.pending: deque = deque()  # (priority, TransferFile, attempt_count)
        self.failed: deque = deque(maxlen=100)  # Keep last 100 failures for reporting
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}  # server_key -> breaker
        self._lock = threading.Lock()
        self.max_retries = max_retries

    def add(self, file: TransferFile, priority: int = 0):
        """Add file to queue with priority (lower = higher priority)."""
        with self._lock:
            self.pending.append((priority, file, 0))  # attempt_count = 0

    def get_next(self, server_key: str) -> Optional[Tuple[TransferFile, int]]:
        """Get next file to transfer, respecting circuit breaker."""
        with self._lock:
            if server_key not in self.circuit_breakers:
                self.circuit_breakers[server_key] = CircuitBreaker()

            breaker = self.circuit_breakers[server_key]
            if not breaker.can_attempt():
                logging.debug(f"Circuit breaker open for {server_key}, skipping")
                return None

            if not self.pending:
                return None

            # Sort by priority and get highest priority item
            self.pending = deque(sorted(self.pending, key=lambda x: (x[0], x[2])))
            priority, file, attempt_count = self.pending.popleft()

            return file, attempt_count

    def record_success(self, file: TransferFile, server_key: str):
        """Record successful transfer."""
        with self._lock:
            if server_key in self.circuit_breakers:
                self.circuit_breakers[server_key].record_success()

    def record_failure(self, file: TransferFile, server_key: str,
                       error: Exception, attempt_count: int) -> bool:
        """
        Record failed transfer. Returns True if should retry.

        Automatically re-queues with exponential backoff if retries remain.
        """
        with self._lock:
            if server_key not in self.circuit_breakers:
                self.circuit_breakers[server_key] = CircuitBreaker()

            state = self.circuit_breakers[server_key].record_failure()

            # Calculate exponential backoff: 5s, 10s, 20s, 40s, 80s
            backoff_delay = min(5 * (2 ** attempt_count), 300)  # Max 5 minutes

            if attempt_count < self.max_retries and state != ConnectionState.CIRCUIT_OPEN:
                logging.warning(
                    f"Transfer failed for {os.path.basename(file.source_path)} "
                    f"(attempt {attempt_count + 1}/{self.max_retries}). "
                    f"Will retry in {backoff_delay}s. Error: {error}"
                )

                def requeue_action():
                    """Re-queues the item after the backoff delay."""
                    with self._lock:
                        priority = attempt_count * 100
                        self.pending.append((priority, file, attempt_count + 1))
                        logging.debug(f"Re-queued {os.path.basename(file.source_path)} after backoff delay.")

                threading.Timer(backoff_delay, requeue_action).start()
                return True
            else:
                # Max retries exceeded or circuit open
                self.failed.append((file, error, attempt_count))
                logging.error(
                    f"Transfer permanently failed for {os.path.basename(file.source_path)} "
                    f"after {attempt_count} attempts: {error}"
                )
                return False

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "pending": len(self.pending),
                "failed": len(self.failed),
                "circuit_states": {
                    key: breaker.state.value
                    for key, breaker in self.circuit_breakers.items()
                }
            }


from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..ui import BaseUIManager


class StallResilienceWatchdog:
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
