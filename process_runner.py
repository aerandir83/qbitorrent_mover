import subprocess
import logging
import re
import time
import select
import os
import fcntl
from typing import List, Callable, Any, Optional, Generator, IO
from utils import RemoteTransferError, _create_safe_command_for_logging

logger = logging.getLogger(__name__)

def _read_until_delimiter(stream: IO[bytes]) -> Generator[bytes, None, None]:
    """
    Generator that reads a stream byte-by-byte and yields a buffer
    whenever a delimiter (b'\\r' or b'\\n') is encountered.
    """
    buffer = bytearray()
    while True:
        chunk = stream.read(1)
        if not chunk:
            if buffer:
                yield buffer
            break

        buffer.extend(chunk)
        if chunk in (b'\r', b'\n') or len(buffer) >= 64:  # Yield on delimiter OR every 64 bytes
            yield buffer
            buffer = bytearray()

def _parse_human_readable_bytes(s: str) -> int:
    """
    Parses a human-readable size string (e.g., '49.41M', '1.2G', '1024') into bytes.
    """
    s = s.strip()
    units = {
        "k": 1024,
        "M": 1024**2,
        "G": 1024**3,
        "T": 1024**4,
    }
    # Check for a unit at the end
    unit = s[-1]
    if unit in units:
        try:
            # Value has a unit (e.g., '49.41M')
            value_str = s[:-1]
            value = float(value_str)
            return int(value * units[unit])
        except ValueError:
            # Malformed string, fall back
            return 0
    else:
        try:
            # No unit, just bytes (e.g., '1024')
            return int(s.replace(',', ''))
        except ValueError:
            return 0

def execute_streaming_command(
    command: List[str],
    torrent_hash: str,
    total_size: int,
    log_transfer: Callable[..., Any],
    _update_transfer_progress: Callable[..., Any],
    heartbeat_callback: Optional[Callable[[], None]] = None,
    timeout_seconds: int = 60
) -> bool:
    logging.info(f"DEBUG: execute_streaming_command called. Heartbeat Callback is: {'PRESENT' if heartbeat_callback else 'MISSING'}")
    """
    Executes a command (like rsync) and streams its stdout to parse progress.
    """
    process = None
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0
        )

        # Set stdout to non-blocking
        fd = process.stdout.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

        # This regex is used to parse rsync's progress output
        # AI-CONTEXT: Rsync Progress Parsing
        # We parse the BYTES_TRANSFERRED from rsync's output to update the UI.
        # The regex must handle comma-separated numbers and human-readable units (e.g., "1.2G").
        # CRITICAL: If this regex breaks, the UI will show 0% progress for rsync transfers.
        # AI-TEST: Verify via tests/test_process_runner_smart.py
        progress_regex = re.compile(r"^\s*([\d,.]+[kMGT]?).*?$") # Regex to find human-readable bytes
        last_transferred_bytes = 0
        last_update_time = time.time()
        last_output_time = time.time()

        read_buffer = bytearray()

        try:
            while True:
                # Check if process is still alive
                return_code = process.poll()

                # We use select to wait for data
                # select timeout 1.0s to allow checking for process exit and timeouts regularly
                rlist, _, _ = select.select([process.stdout], [], [], 1.0)

                if rlist:
                    # Data is available
                    try:
                        chunk = os.read(fd, 4096)
                    except BlockingIOError:
                        chunk = b""

                    if chunk:
                        last_output_time = time.time()
                        read_buffer.extend(chunk)

                        # Process buffer for lines
                        while True:
                            # Look for delimiters \n or \r
                            idx_n = read_buffer.find(b'\n')
                            idx_r = read_buffer.find(b'\r')

                            split_idx = -1
                            if idx_n != -1 and idx_r != -1:
                                split_idx = min(idx_n, idx_r)
                            elif idx_n != -1:
                                split_idx = idx_n
                            elif idx_r != -1:
                                split_idx = idx_r

                            if split_idx != -1:
                                line_bytes = read_buffer[:split_idx+1]
                                del read_buffer[:split_idx+1]
                            else:
                                break

                            if heartbeat_callback:
                                heartbeat_callback()

                            line = line_bytes.decode('utf-8', errors='replace').strip()
                            if not line:
                                continue

                            logger.debug(f"({torrent_hash[:10]}) [RSYNC_PROGRESS] {line}")

                            match = progress_regex.match(line)
                            if match:
                                human_readable_bytes_str = match.group(1)
                                current_transferred_bytes = _parse_human_readable_bytes(human_readable_bytes_str)
                                log_transfer(torrent_hash, f"[DEBUG] Parsed bytes: {current_transferred_bytes}")

                                transferred_delta = current_transferred_bytes - last_transferred_bytes
                                if transferred_delta > 0:
                                    elapsed_time = time.time() - last_update_time
                                    if elapsed_time > 0:
                                        speed = transferred_delta / elapsed_time
                                        last_update_time = time.time()

                                last_transferred_bytes = current_transferred_bytes
                                progress = (current_transferred_bytes / total_size) if total_size > 0 else 0
                                _update_transfer_progress(
                                    torrent_hash,
                                    progress,
                                    current_transferred_bytes,
                                    total_size
                                )
                    else:
                        # select says ready but read returns empty -> EOF
                        break
                else:
                    # No data (select timeout)
                    if return_code is not None:
                        # Process finished
                        break

                    # Check timeout
                    if time.time() - last_output_time > timeout_seconds:
                        logger.error(f"Process timed out after {timeout_seconds}s of silence.")
                        # Sanitize command before raising exception to hide passwords in traceback
                        safe_cmd = _create_safe_command_for_logging(command)
                        raise subprocess.TimeoutExpired(safe_cmd, timeout_seconds)

        finally:
            if process.poll() is None:
                logging.warning("Cleaning up orphaned rsync process...")
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()

        process.wait()
        stderr_output_bytes = process.stderr.read() if process.stderr else b""
        stderr_output = stderr_output_bytes.decode('utf-8', errors='replace')

        if process.returncode == 0 or process.returncode == 24: # 0=success, 24=vanished files
            # Ensure UI completes to 100%
            if total_size > 0 and last_transferred_bytes < total_size:
                _update_transfer_progress(torrent_hash, 1.0, total_size, total_size)
            return True
        else:
            log_transfer(torrent_hash, f"[bold red]Rsync FAILED (code {process.returncode})[/bold red]")
            raise RemoteTransferError(f"Rsync failed (exit {process.returncode}): {stderr_output}")

    except subprocess.TimeoutExpired:
        logger.error(f"Rsync process timed out for torrent {torrent_hash}")
        if process:
            process.kill()
        raise RemoteTransferError(f"Rsync timed out for torrent {torrent_hash}")
    except Exception as e:
        logger.error(f"An exception occurred during rsync for torrent {torrent_hash}: {e}", exc_info=True)
        if process:
            process.kill()
        raise RemoteTransferError(f"Rsync exception: {e}")
