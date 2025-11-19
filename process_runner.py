import subprocess
import logging
import re
import time
from typing import List, Callable, Any, Optional, Generator, IO
from utils import RemoteTransferError

logger = logging.getLogger(__name__)

def _read_until_delimiter(stream: IO[bytes]) -> Generator[bytes, None, None]:
    """
    Generator that reads a stream byte-by-byte and yields a buffer
    whenever a delimiter (b'\\r' or b'\\n') is encountered.
    """
    buffer = bytearray()
    while True:
        char = stream.read(1)
        if not char:
            if buffer:
                yield buffer
            break

        buffer.extend(char)
        if char in (b'\r', b'\n'):
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
    heartbeat_callback: Optional[Callable[[], None]] = None
) -> bool:
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

        # This regex is used to parse rsync's progress output
        progress_regex = re.compile(r"^\s*([\d,.]+[kMGT]?).*?$") # Regex to find human-readable bytes
        last_transferred_bytes = 0
        last_update_time = time.time()

        if process.stdout:
            for line_buffer in _read_until_delimiter(process.stdout):
                if heartbeat_callback:
                    heartbeat_callback()

                line = line_buffer.decode('utf-8', errors='replace').strip()
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
