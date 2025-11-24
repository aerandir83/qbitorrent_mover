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
    Executes a command (like rsync) and keeps it alive.

    Legacy Note: This function used to parse rsync output for progress updates.
    That logic has been removed in favor of direct file size monitoring via SpeedMonitor.
    It now serves purely as a robust process runner that handles buffering and timeouts.
    """
    # Regex to capture rsync progress:
    # Captures: 1=Bytes, 2=Percentage, 3=Speed, 4=ETA
    # Example:  41,943,040   3%   39.08MB/s    0:00:20
    progress_pattern = re.compile(r'\s*([\d,]+)\s+(\d+)%\s+([0-9.]+[kMGTP]?B/s)\s+([0-9:]+)')

    process = None
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0
        )

        # Set stdout to non-blocking
        fd_out = process.stdout.fileno()
        fl_out = fcntl.fcntl(fd_out, fcntl.F_GETFL)
        fcntl.fcntl(fd_out, fcntl.F_SETFL, fl_out | os.O_NONBLOCK)

        # Set stderr to non-blocking
        fd_err = process.stderr.fileno()
        fl_err = fcntl.fcntl(fd_err, fcntl.F_GETFL)
        fcntl.fcntl(fd_err, fcntl.F_SETFL, fl_err | os.O_NONBLOCK)

        last_activity_time = time.time()

        read_buffer = bytearray()
        stderr_buffer = bytearray()

        stdout_open = True
        stderr_open = True

        try:
            while stdout_open or stderr_open:
                # Check if process is still alive
                return_code = process.poll()

                check_list = []
                if stdout_open:
                    check_list.append(process.stdout)
                if stderr_open:
                    check_list.append(process.stderr)

                if not check_list:
                    break

                # We use select to wait for data
                # select timeout 1.0s to allow checking for process exit and timeouts regularly
                rlist, _, _ = select.select(check_list, [], [], 1.0)

                if rlist:
                    # STDOUT Handling
                    if process.stdout in rlist:
                        try:
                            chunk = os.read(fd_out, 4096)
                        except BlockingIOError:
                            chunk = b""

                        if chunk:
                            last_activity_time = time.time()
                            if heartbeat_callback:
                                heartbeat_callback()

                            # Log raw chunk
                            logger.debug(f"({torrent_hash[:10]}) [RSYNC_RAW_STDOUT] {chunk!r}")

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

                                line = line_bytes.decode('utf-8', errors='replace').strip()
                                if not line:
                                    continue

                                # Parse progress
                                match = progress_pattern.search(line)
                                if match:
                                    try:
                                        current_bytes_str = match.group(1).replace(',', '')
                                        current_bytes = int(current_bytes_str)
                                        percentage_str = match.group(2)
                                        percentage = int(percentage_str)

                                        # Update progress (0.0 to 1.0)
                                        progress_float = min(float(percentage) / 100.0, 1.0)

                                        _update_transfer_progress(torrent_hash, progress_float, current_bytes, total_size)
                                    except ValueError:
                                        pass # Failed to parse numbers, ignore

                                # Log trace at high verbosity
                                # We still need to read stdout to prevent deadlock
                                logger.log(logging.NOTSET, f"({torrent_hash[:10]}) [RSYNC_STDOUT] {line}")

                        else:
                            # EOF stdout
                            stdout_open = False

                    # STDERR Handling
                    if process.stderr in rlist:
                        try:
                            chunk_err = os.read(fd_err, 4096)
                        except BlockingIOError:
                            chunk_err = b""

                        if chunk_err:
                            last_activity_time = time.time()
                            decoded_err = chunk_err.decode('utf-8', errors='replace')
                            logger.debug(f"({torrent_hash[:10]}) [RSYNC_ERR] {decoded_err}")
                            stderr_buffer.extend(chunk_err)
                        else:
                            # EOF stderr
                            stderr_open = False

                else:
                    # No data (select timeout)
                    if return_code is not None:
                        # Process finished
                        break

                    # Check timeout
                    if time.time() - last_activity_time > timeout_seconds:
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
        # Use accumulated stderr
        stderr_output = stderr_buffer.decode('utf-8', errors='replace')

        if process.returncode == 0 or process.returncode == 24: # 0=success, 24=vanished files
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
