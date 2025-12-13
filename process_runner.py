import subprocess
import logging
import re
import time
import select
import os
try:
    import fcntl
except ImportError:
    fcntl = None
from typing import List, Callable, Any, Optional, Generator, IO, Set
import threading
from utils import RemoteTransferError, _create_safe_command_for_logging

logger = logging.getLogger(__name__)

# Global state to track active processes for graceful shutdown
_active_processes: Set[subprocess.Popen] = set()
_process_lock = threading.Lock()
_shutting_down = False

def _register_process(process: subprocess.Popen) -> None:
    """Registers a process as active."""
    with _process_lock:
        _active_processes.add(process)

def _unregister_process(process: subprocess.Popen) -> None:
    """Unregisters a process."""
    with _process_lock:
        _active_processes.discard(process)

def stop_all_processes() -> None:
    """
     signals that the application is shutting down and terminates all tracked processes.
    """
    global _shutting_down
    _shutting_down = True
    logging.info("Stopping all active processes...")
    with _process_lock:
        for p in list(_active_processes): # Copy set to iterate
            try:
                if p.poll() is None:
                    p.terminate()
            except Exception as e:
                logging.warning(f"Failed to terminate process: {e}")


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
    timeout_seconds: int = 60,
    speed_callback: Optional[Callable[[float], None]] = None
) -> bool:
    logging.debug(f"DEBUG: execute_streaming_command called. Heartbeat: {'YES' if heartbeat_callback else 'NO'}")
    """
    Executes a command (like rsync) and keeps it alive.
    """
    # Regex to capture rsync progress:
    # Captures: 1=Bytes, 2=Percentage, 3=Speed, 4=ETA
    # Updated to handle human-readable bytes (e.g. 577.83M)
    progress_pattern = re.compile(r'\s*([0-9.,]+[a-zA-Z]?)\s+(\d+)%\s+([0-9.]+[kMGTP]?B/s)\s+([0-9:]+)')

    process = None
    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        
        # Register the process immediately
        _register_process(process)
        
        # Immediate check in case shutdown started while we were creating the process
        if _shutting_down:
            logger.warning("Shutdown detected during process creation. Terminating.")
            process.terminate()
            raise KeyboardInterrupt("Shutdown requested")

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
                            chunk = os.read(fd_out, 32768)
                        except BlockingIOError:
                            chunk = b""
                        
                        if chunk:
                            last_activity_time = time.time()
                            if heartbeat_callback:
                                heartbeat_callback()

                            # Log raw chunk
                            logger.debug(f"({torrent_hash[:10]}) [RSYNC_RAW_STDOUT] {chunk!r}")

                            read_buffer.extend(chunk)

                            # Process lines from buffer
                            while True:
                                try:
                                    # Look for delimiters \n or \r
                                    idx_n = read_buffer.find(b'\n')
                                    idx_r = read_buffer.find(b'\r')
                                except Exception:
                                    break

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
                                
                                # DEBUG: Log the line we are trying to parse
                                # logging.info(f"DEBUG_PARSE: '{line}'")

                                # Parse progress
                                match = progress_pattern.search(line)
                                if match:
                                    try:
                                        # Parse Bytes (Group 1) which might be human readable (577.83M)
                                        bytes_str = match.group(1).replace(',', '')
                                        current_bytes = 0
                                        
                                        # Parse multipliers for bytes
                                        byte_mults = {'k': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4, 'P': 1024**5}
                                        # Check for suffix
                                        matched_mult = 1
                                        val_part_bytes = bytes_str
                                        for suffix, mult in byte_mults.items():
                                            if bytes_str.endswith(suffix):
                                                matched_mult = mult
                                                val_part_bytes = bytes_str[:-1] # Remove suffix
                                                break
                                        
                                        try:
                                             current_bytes = int(float(val_part_bytes) * matched_mult)
                                        except ValueError:
                                             pass

                                        # AI-FIX: Parse Speed directly from Rsync
                                        speed_val = 0.0
                                        try:
                                            speed_str = match.group(3)
                                            unit_multipliers = {
                                                'kB/s': 1024, 'MB/s': 1024**2, 'GB/s': 1024**3, 'TB/s': 1024**4, 'B/s': 1
                                            }
                                            for unit, mult in unit_multipliers.items():
                                                if unit in speed_str:
                                                    val_part = speed_str.replace(unit, '')
                                                    speed_val = float(val_part) * mult
                                                    break
                                            
                                            # Update callback if present
                                            if speed_callback and speed_val > 0:
                                                speed_callback(speed_val)

                                        except (ValueError, IndexError):
                                            pass

                                        percentage_str = match.group(2)
                                        percentage = int(percentage_str)

                                        # Update progress (0.0 to 1.0)
                                        progress_float = min(float(percentage) / 100.0, 1.0)
                                        
                                        # Pass parsed speed and status to update callback
                                        # Note: We assume "Transferring" if we carry specific speed info. 
                                        # If it was checking, speed would likely be read-speed or nil.
                                        _update_transfer_progress(torrent_hash, progress_float, current_bytes, total_size, speed=speed_val, status_text="Transferring")

                                    except ValueError:
                                        pass # Failed to parse numbers, ignore

                                # Log trace at high verbosity ONLY if needed (disabled to prevent IO bottleneck)
                                # logger.log(logging.NOTSET, f"({torrent_hash[:10]}) [RSYNC_STDOUT] {line}")
                                pass

                        else:
                            # EOF stdout
                            stdout_open = False

                    # STDERR Handling
                    if process.stderr in rlist:
                        try:
                            # Increase chunk size to 32KB
                            chunk_err = os.read(fd_err, 32768)
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
            
            if process:
                _unregister_process(process)

        process.wait()
        # Use accumulated stderr
        stderr_output = stderr_buffer.decode('utf-8', errors='replace')

        if _shutting_down:
             raise KeyboardInterrupt("Shutdown requested")

        if process.returncode == 0 or process.returncode == 24: # 0=success, 24=vanished files

            return True
        else:
            log_transfer(torrent_hash, f"[bold red]Rsync FAILED (code {process.returncode})[/bold red]")
            raise RemoteTransferError(f"Rsync failed (exit {process.returncode}): {stderr_output}")

    except KeyboardInterrupt:
        logger.warning(f"Process interrupted by user. Killing rsync process for torrent {torrent_hash}")
        if process:
            try:
                process.kill()
            except OSError:
                pass
        raise
    except subprocess.TimeoutExpired:
        logger.error(f"Rsync process timed out for torrent {torrent_hash}")
        if process:
            try:
                process.kill()
            except OSError:
                pass
        raise RemoteTransferError(f"Rsync timed out for torrent {torrent_hash}")
    except Exception as e:
        logger.error(f"An exception occurred during rsync for torrent {torrent_hash}: {e}", exc_info=True)
        if process:
            try:
                process.kill()
            except OSError:
                pass
        raise RemoteTransferError(f"Rsync exception: {e}")
