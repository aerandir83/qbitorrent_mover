"""Provides a resilient queuing system for handling file transfers.

This module contains classes designed to manage and execute file transfers in a
robust manner, particularly in environments with unreliable network connections.
It includes a `CircuitBreaker` to prevent cascading failures and a
`ResilientTransferQueue` that automatically retries failed transfers with
exponential backoff.

Classes:
    TransferFile: A dataclass representing a file to be transferred.
    ConnectionState: An Enum for the states of the CircuitBreaker.
    CircuitBreaker: A mechanism to stop transfer attempts to an unresponsive server.
    ResilientTransferQueue: A thread-safe queue for managing file transfers with
        retry and circuit breaker logic.
"""
import os
import logging
from collections import deque
from enum import Enum
import time
import threading
from typing import Optional, Any, Dict, Tuple
from dataclasses import dataclass

@dataclass
class TransferFile:
    """A dataclass representing a single file to be transferred.

    This class standardizes the representation of a file transfer operation,
    containing all necessary information for various transfer strategies.

    Attributes:
        source_path (str): The full path to the source file.
        dest_path (str): The full path to the destination file.
        size (int): The size of the file in bytes.
        torrent_hash (str): The hash of the parent torrent, used for tracking.
    """
    source_path: str
    dest_path: str
    size: int
    torrent_hash: str

    @property
    def normalized_source(self) -> str:
        """Returns the source path with surrounding quotes stripped."""
        return self.source_path.strip('\'"')

    @property
    def normalized_dest(self) -> str:
        """Returns the destination path with surrounding quotes stripped."""
        return self.dest_path.strip('\'"')

class ConnectionState(Enum):
    """Enumeration for the state of a `CircuitBreaker`."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CIRCUIT_OPEN = "circuit_open"

class CircuitBreaker:
    """Prevents cascading failures by temporarily halting operations to a failing service.

    This class implements the Circuit Breaker pattern. It tracks the number of
    consecutive failures for a given service (e.g., an SFTP server). If the
    number of failures exceeds a threshold, the circuit "opens," and further
    attempts are blocked for a timeout period. After the timeout, the circuit
    moves to a "half-open" (degraded) state, allowing a limited number of test
    requests. If these succeed, the circuit closes again (healthy).

    Attributes:
        failure_threshold (int): The number of failures required to open the circuit.
        timeout_seconds (int): The duration the circuit remains open before allowing retries.
    """
    def __init__(self, failure_threshold: int = 5, timeout_seconds: int = 60):
        """Initializes the CircuitBreaker.

        Args:
            failure_threshold: The number of consecutive failures to tolerate before opening the circuit.
            timeout_seconds: The time in seconds to wait before transitioning from OPEN to DEGRADED.
        """
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.state = ConnectionState.HEALTHY
        self._lock = threading.Lock()

    def record_success(self) -> None:
        """Records a successful operation, decrementing the failure count."""
        with self._lock:
            self.failure_count = max(0, self.failure_count - 1)
            if self.failure_count == 0:
                self.state = ConnectionState.HEALTHY

    def record_failure(self) -> ConnectionState:
        """Records a failed operation, incrementing the failure count and updating the state.

        Returns:
            The new state of the circuit breaker (`ConnectionState`).
        """
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.failure_count >= self.failure_threshold:
                self.state = ConnectionState.CIRCUIT_OPEN
                logging.warning(f"Circuit breaker opened after {self.failure_count} failures.")
            elif self.failure_count >= self.failure_threshold // 2:
                self.state = ConnectionState.DEGRADED
            return self.state

    def can_attempt(self) -> bool:
        """Determines if an operation should be attempted based on the circuit's state.

        Returns:
            `True` if the circuit is healthy or degraded.
            `True` if the circuit is open but the timeout has expired (moves to degraded).
            `False` if the circuit is open and the timeout has not expired.
        """
        with self._lock:
            if self.state != ConnectionState.CIRCUIT_OPEN:
                return True

            if time.time() - self.last_failure_time > self.timeout_seconds:
                logging.info("Circuit breaker timeout elapsed, attempting half-open state.")
                self.state = ConnectionState.DEGRADED
                self.failure_count = self.failure_threshold // 2
                return True

            return False

class ResilientTransferQueue:
    """A thread-safe queue that retries failed transfers with exponential backoff.

    This class manages a queue of `TransferFile` objects. When a transfer fails,
    it is automatically re-queued to be tried again later. The delay between
    retries increases exponentially to avoid overwhelming a struggling server.
    It also integrates with a `CircuitBreaker` for each server to prevent repeated
    attempts to an unresponsive host.

    Attributes:
        max_retries (int): The maximum number of times to retry a single file transfer.
    """
    def __init__(self, max_retries: int = 5):
        """Initializes the ResilientTransferQueue.

        Args:
            max_retries: The maximum number of retry attempts for a failed transfer.
        """
        self.pending: deque = deque()  # Stores tuples of (priority, TransferFile, attempt_count)
        self.failed: deque = deque(maxlen=100)  # Stores permanently failed transfers for reporting
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}  # One breaker per server_key
        self._lock = threading.Lock()
        self.max_retries = max_retries

    def add(self, file: TransferFile, priority: int = 0) -> None:
        """Adds a file to the transfer queue.

        Args:
            file: The `TransferFile` object to be added.
            priority: The priority of the transfer (lower number is higher priority).
        """
        with self._lock:
            self.pending.append((priority, file, 0))  # initial attempt_count is 0

    def get_next(self, server_key: str) -> Optional[Tuple[TransferFile, int]]:
        """Retrieves the next file to transfer, respecting the server's circuit breaker.

        Args:
            server_key: A unique identifier for the server (e.g., 'host:port').

        Returns:
            A tuple of (`TransferFile`, `attempt_count`) if a file is available and the
            circuit is closed. Otherwise, returns `None`.
        """
        with self._lock:
            if server_key not in self.circuit_breakers:
                self.circuit_breakers[server_key] = CircuitBreaker()

            breaker = self.circuit_breakers[server_key]
            if not breaker.can_attempt():
                logging.debug(f"Circuit breaker open for {server_key}, skipping get_next.")
                return None

            if not self.pending:
                return None

            # Sort by priority and then by attempt count to process highest priority first
            self.pending = deque(sorted(self.pending, key=lambda x: (x[0], x[2])))
            _priority, file, attempt_count = self.pending.popleft()

            return file, attempt_count

    def record_success(self, file: TransferFile, server_key: str) -> None:
        """Records a successful transfer and informs the relevant circuit breaker.

        Args:
            file: The `TransferFile` that was successfully transferred.
            server_key: The identifier for the server involved in the transfer.
        """
        with self._lock:
            if server_key in self.circuit_breakers:
                self.circuit_breakers[server_key].record_success()

    def record_failure(self, file: TransferFile, server_key: str, error: Exception, attempt_count: int) -> bool:
        """Records a failed transfer, re-queues it if retries remain, and updates the circuit breaker.

        Args:
            file: The `TransferFile` that failed.
            server_key: The identifier for the server that failed.
            error: The exception that caused the failure.
            attempt_count: The number of attempts made so far for this file.

        Returns:
            `True` if the file was re-queued for another attempt, `False` if it has
            exceeded its max retries and failed permanently.
        """
        with self._lock:
            if server_key not in self.circuit_breakers:
                self.circuit_breakers[server_key] = CircuitBreaker()

            state = self.circuit_breakers[server_key].record_failure()

            # Exponential backoff: 5s, 10s, 20s, etc., capped at 5 minutes
            backoff_delay = min(5 * (2 ** attempt_count), 300)

            if attempt_count < self.max_retries and state != ConnectionState.CIRCUIT_OPEN:
                logging.warning(
                    f"Transfer failed for {os.path.basename(file.source_path)} "
                    f"(attempt {attempt_count + 1}/{self.max_retries}). "
                    f"Will retry in {backoff_delay}s. Error: {error}"
                )
                # Re-queue the item to be processed after the backoff delay
                def requeue_action():
                    with self._lock:
                        # Increase priority for retries to handle them sooner
                        priority = attempt_count * 100
                        self.pending.append((priority, file, attempt_count + 1))
                        logging.debug(f"Re-queued {os.path.basename(file.source_path)} after backoff.")

                threading.Timer(backoff_delay, requeue_action).start()
                return True
            else:
                # Max retries exceeded or circuit is open, mark as permanently failed
                self.failed.append((file, error, attempt_count))
                logging.error(
                    f"Transfer permanently failed for {os.path.basename(file.source_path)} "
                    f"after {attempt_count + 1} attempts: {error}"
                )
                return False

    def get_stats(self) -> Dict[str, Any]:
        """Retrieves statistics about the queue's current state.

        Returns:
            A dictionary containing the number of pending and failed items, and
            the current state of all tracked circuit breakers.
        """
        with self._lock:
            return {
                "pending": len(self.pending),
                "failed": len(self.failed),
                "circuit_states": {
                    key: breaker.state.value
                    for key, breaker in self.circuit_breakers.items()
                }
            }
