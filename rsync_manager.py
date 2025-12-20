"""
rsync_manager.py - High-performance Rsync Logic and Execution

Encapsulates:
1. Optimization logic (Flag selection, SSH options)
2. Execution logic (Retries, Error handling, Process management)
3. Verification logic
"""

import os
import time
import shutil
import logging
import subprocess
from pathlib import Path
from typing import List, Optional, Callable, Any, Dict, Union

import process_runner
from utils import RemoteTransferError, _create_safe_command_for_logging
# Lazy import for ssh_manager to avoid potential circular imports if they exist, 
# though clean architecture should prevent it.
import ssh_manager 

logger = logging.getLogger(__name__)

class RsyncSpeedOptimizer:
    """
    Optimizes rsync flags for maximum speed within the 128KB block limit.
    """
    
    # Maximum safe block size for most rsync versions
    MAX_SAFE_BLOCK_SIZE = 128 * 1024  # 128KB
    
    def __init__(self, total_size_bytes: int, is_repair: bool = False):
        self.total_size = total_size_bytes
        self.is_repair = is_repair
    
    def build_flags(self) -> List[str]:
        """Build optimized rsync flags."""
        flags = []
        
        # === CORE FLAGS ===
        flags.extend([
            '-r',   # Recursive
            '-l',   # Preserve symlinks
            '-t',   # Preserve modification times (needed for rsync delta logic)
            '-D',   # Preserve devices and special files
        ])
        
        # === PERFORMANCE FLAGS ===
        flags.extend([
            '-h',           # Human-readable sizes
            '-P',           # Progress + partial (keep incomplete files)
            '--inplace',    # Update files in-place (faster, less disk usage)
        ])
        
        # Block size: Use max safe value
        flags.append(f'--block-size={self.MAX_SAFE_BLOCK_SIZE}')
        
        # === GAME CHANGER: --whole-file vs delta sync ===
        if self.is_repair:
            flags.append('--no-whole-file')
            flags.append('--checksum')  # Verify data integrity
            # logging.info("Using delta sync mode (repair/resume)")
        else:
            flags.append('--whole-file')
            # logging.info("Using whole-file mode (3x faster for new files)")
        
        # === GAME CHANGER #2: --no-inc-recursive ===
        if self.total_size > 1024**3:  # > 1GB
            flags.append('--no-inc-recursive')
        
        # === SKIP METADATA ===
        flags.extend(['--no-perms', '--no-owner', '--no-group'])
        
        # === COMPRESSION ===
        flags.append('--compress-level=0')
        
        # === PROGRESS REPORTING ===
        flags.extend(['--info=progress2', '-v'])
        
        return flags
    
    def build_ssh_options(self, port: int = 22) -> str:
        """Build optimized SSH options."""
        opts = [
            f'-p {port}',
            '-c aes128-gcm@openssh.com,aes128-ctr',
            '-o Compression=no',
            '-o StrictHostKeyChecking=no',
            '-o UserKnownHostsFile=/dev/null',
            '-o ServerAliveInterval=60',
            '-o ServerAliveCountMax=30',
            '-o TCPKeepAlive=yes',
            '-o ConnectTimeout=30',
        ]
        return 'ssh ' + ' '.join(opts)

    def build_command_base(self, password: str, port: int, extra_flags: List[str] = None) -> List[str]:
        """
        Builds the base command list (stdbuf...rsync...flags) WITHOUT src/dest.
        """
        cmd = ['stdbuf', '-o0']
        if password:
            cmd.extend(['sshpass', '-p', password])
        
        cmd.append('rsync')
        cmd.extend(self.build_flags())
        
        if extra_flags:
            cmd.extend(extra_flags)
            
        cmd.extend(['-e', self.build_ssh_options(port)])
        return cmd


class RsyncDownloader:
    """
    Handles the execution of an Rsync Download (Remote -> Local).
    Includes logic for: retries, atomic fallbacks, process execution, and verification.
    """
    
    MAX_RETRY_ATTEMPTS = 3
    
    def __init__(
        self,
        torrent_hash: str,
        total_size: int,
        log_transfer: Callable,
        progress_callback: Callable, # _update_transfer_progress
        file_tracker: Any, # FileTransferTracker
        heartbeat_callback: Optional[Callable] = None,
        ui_actions: Optional[Any] = None # Object with update_external_speed, update_torrent_progress
    ):
        self.torrent_hash = torrent_hash
        self.total_size = total_size
        self.log_transfer = log_transfer
        self.progress_callback = progress_callback
        self.file_tracker = file_tracker
        self.heartbeat_callback = heartbeat_callback
        self.ui_actions = ui_actions

    def transfer(
        self,
        remote_path: str,
        local_path: str,
        sftp_config: Dict[str, Any], # host, port, username, password
        rsync_options: List[str],
        dry_run: bool = False,
        timeout: int = 600,
        verify: bool = True,
        force_integrity: bool = False
    ):
        remote_path = remote_path.strip('\'"')
        file_name = os.path.basename(remote_path)
        local_parent_dir = os.path.dirname(local_path)
        
        # Unpack config
        host = sftp_config['host']
        port = int(sftp_config.get('port', 22))
        username = sftp_config['username']
        password = sftp_config['password']
        
        remote_spec = f"{username}@{host}:{remote_path}"
        
        # 1. Prepare Optimizer
        optimizer = RsyncSpeedOptimizer(self.total_size, is_repair=force_integrity)
        
        command_base = optimizer.build_command_base(password, port, rsync_options)
        
        adaptive_timeout = timeout

        for attempt in range(1, self.MAX_RETRY_ATTEMPTS + 1):
            try:
                # 2. Atomic Checks (Skipped on attempt 1 unless forced?)
                # Actually original logic: attempt > 1 checks for stalled files.
                self._handle_atomic_fallback(local_path, remote_path, attempt)

                rsync_command = [*command_base, remote_spec, local_parent_dir]

                if dry_run:
                    self._handle_dry_run(rsync_command, file_name)
                    return

                # 3. Execution
                self.log_transfer(self.torrent_hash, f"Starting rsync transfer for '{file_name}' (attempt {attempt}/{self.MAX_RETRY_ATTEMPTS})")
                 # Set status to Checking before Rsync starts
                self.progress_callback(self.torrent_hash, 0, 0, self.total_size, status_text="Checking/Resuming")
                
                logger.debug(f"Executing rsync: {' '.join(_create_safe_command_for_logging(rsync_command))}")

                success = process_runner.execute_streaming_command(
                    command=rsync_command,
                    torrent_hash=self.torrent_hash,
                    total_size=self.total_size,
                    log_transfer=self.log_transfer,
                    _update_transfer_progress=self.progress_callback,
                    heartbeat_callback=self.heartbeat_callback,
                    timeout_seconds=adaptive_timeout,
                    speed_callback=self._speed_handler(remote_path),
                    current_file_callback=self._file_handler(remote_path)
                )

                if success:
                    if verify:
                        verified = self._verify_transfer(local_path, remote_path, sftp_config, file_name, attempt)
                        if verified:
                            return
                        else:
                            # Verification failed, retry loop continues
                            continue 
                    else:
                        self.log_transfer(self.torrent_hash, f"Rsync transfer completed (no verify) for {file_name}")
                        return

                # Process runner returned False without exception ??
                raise RemoteTransferError("Rsync execution failed (process returned failure).")

            except Exception as e:
                if not self._handle_exception(e, attempt, remote_path, file_name):
                    raise
                # Update timeout for next attempt
                adaptive_timeout += 60

        # End of loop
        self.file_tracker.record_corruption(self.torrent_hash, remote_path)
        raise RemoteTransferError(f"Rsync transfer failed for {file_name} after {self.MAX_RETRY_ATTEMPTS} attempts")

    def _handle_atomic_fallback(self, local_path: str, remote_path: str, attempt: int):
        """Checks for corruption or stalled partial files and cleans them up."""
        is_corrupted = self.file_tracker.is_corrupted(self.torrent_hash, remote_path)
        is_stalled = False
        
        if os.path.exists(local_path) and attempt > 1:
            try:
                if os.path.getsize(local_path) < 1024:
                    is_stalled = True
            except OSError:
                pass

        if is_corrupted or is_stalled:
            reason = "marked corrupted" if is_corrupted else "stalled partial file"
            logging.warning(f"Atomic Fallback: Removing {reason} before retry: {local_path}")
            try:
                if os.path.isdir(local_path):
                    shutil.rmtree(local_path)
                else:
                    os.remove(local_path)
            except OSError as e:
                logging.warning(f"Failed to remove corrupted/stalled file {local_path}: {e}")

    def _verify_transfer(self, local_path: str, remote_path: str, sftp_config: dict, file_name: str, attempt: int) -> bool:
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local file not found after rsync: {local_path}")

        local_size = os.path.getsize(local_path) if os.path.isfile(local_path) else \
                     sum(f.stat().st_size for f in Path(local_path).rglob('*') if f.is_file())

        try:
            # Create a localized pool just for verification
            # Note: Ideally we should pass an open connection, but rsync is separate from SFTP.
            temp_pool = ssh_manager.SSHConnectionPool(
                host=sftp_config['host'], 
                port=int(sftp_config.get('port', 22)), 
                username=sftp_config['username'],
                password=sftp_config['password'], 
                max_size=1
            )
            with temp_pool.get_connection() as (_, ssh):
                from ssh_manager import batch_get_remote_sizes
                remote_sizes = batch_get_remote_sizes(ssh, [remote_path])
                remote_size = remote_sizes.get(remote_path, 0)
            temp_pool.close_all()

            size_diff = abs(local_size - remote_size)
            size_diff_percent = (size_diff / remote_size * 100) if remote_size > 0 else 0

            if size_diff_percent < 0.1:
                self.log_transfer(self.torrent_hash, f"Rsync transfer completed and verified for {file_name}")
                return True
            else:
                logging.warning(f"Size mismatch after rsync: {size_diff} bytes difference ({size_diff_percent:.2f}%)")
                if attempt < self.MAX_RETRY_ATTEMPTS:
                    logging.info("Will retry with delta sync...")
                    return False
                else:
                    raise Exception(f"Size verification failed after {self.MAX_RETRY_ATTEMPTS} attempts")

        except Exception as e:
            logging.warning(f"Could not verify remote size: {e}. Assuming transfer is OK based on rsync exit code.")
            self.log_transfer(self.torrent_hash, f"Rsync transfer completed (verification failed) for {file_name}")
            return True

    def _handle_exception(self, e: Exception, attempt: int, remote_path: str, file_name: str) -> bool:
        """Returns True if should retry, False if should raise."""
        if isinstance(e, KeyboardInterrupt):
            logging.warning(f"Rsync transfer for '{file_name}' interrupted by user.")
            raise e
        
        logging.error(f"Rsync attempt {attempt} failed: {e}", exc_info=(attempt == self.MAX_RETRY_ATTEMPTS))
        
        if attempt < self.MAX_RETRY_ATTEMPTS:
            sleep_time = [10, 30, 60][min(attempt - 1, 2)]
            logging.warning(f"Retrying in {sleep_time} seconds...")
            time.sleep(sleep_time)
            return True
            
        return False

    def _handle_dry_run(self, cmd: List[str], file_name: str):
        logging.info(f"[DRY_RUN] Would execute: {' '.join(cmd)}")
        if self.total_size > 0:
            self.progress_callback(self.torrent_hash, 1.0, self.total_size, self.total_size, status_text="Dry Run")
        self.log_transfer(self.torrent_hash, f"[DRY RUN] Completed rsync transfer for {file_name}")

    def _speed_handler(self, remote_path: str):
        if not self.ui_actions:
            return None
        def cb(speed_val: float):
            if hasattr(self.ui_actions, 'update_external_speed'):
                self.ui_actions.update_external_speed(remote_path, speed_val)
        return cb

    def _file_handler(self, remote_path: str):
        if not self.ui_actions:
            return None
        def cb(filename: str):
             # Normalize path logic from original
             full_path = str(Path(remote_path) / filename).replace('\\', '/')
             if full_path.endswith('/'): full_path = full_path[:-1]
             self.ui_actions.update_torrent_progress(
                 self.torrent_hash, 
                 0, 
                 "download", 
                 file_name=full_path
             )
        return cb


class RsyncUploader(RsyncDownloader):
    """
    Handles the execution of an Rsync Upload (Local -> Remote).
    """
    def transfer(
        self,
        local_path: str,
        remote_path: str,
        dest_config: Dict[str, Any], # host, port, username, password
        rsync_options: List[str],
        dry_run: bool = False,
        timeout: int = 600,
        verify: bool = True,
        force_integrity: bool = False
    ):
        file_name = os.path.basename(local_path)
        
        # Unpack config
        host = dest_config['host']
        port = int(dest_config.get('port', 22))
        username = dest_config['username']
        password = dest_config['password']
        
        remote_parent_dir = os.path.dirname(remote_path)
        cleaned_remote_parent_dir = remote_parent_dir.strip('\'"')
        remote_spec = f"{username}@{host}:{cleaned_remote_parent_dir}"

        # 1. Prepare Optimizer
        optimizer = RsyncSpeedOptimizer(self.total_size, is_repair=force_integrity)
        command_base = optimizer.build_command_base(password, port, rsync_options)
        
        adaptive_timeout = timeout

        for attempt in range(1, self.MAX_RETRY_ATTEMPTS + 1):
            try:
                rsync_command = [*command_base, local_path, remote_spec]

                if dry_run:
                    self._handle_dry_run(rsync_command, file_name)
                    return

                self.log_transfer(self.torrent_hash, f"Starting rsync upload for '{file_name}' (attempt {attempt}/{self.MAX_RETRY_ATTEMPTS})")
                
                logger.debug(f"Executing rsync upload: {' '.join(_create_safe_command_for_logging(rsync_command))}")

                success = process_runner.execute_streaming_command(
                    command=rsync_command,
                    torrent_hash=self.torrent_hash,
                    total_size=self.total_size,
                    log_transfer=self.log_transfer,
                    _update_transfer_progress=self.progress_callback,
                    heartbeat_callback=self.heartbeat_callback,
                    timeout_seconds=adaptive_timeout,
                    speed_callback=self._speed_handler(local_path),
                    current_file_callback=None
                )

                if success:
                    self.log_transfer(self.torrent_hash, f"[green]Rsync upload complete: {file_name}[/green]")
                    return

                raise RemoteTransferError("Rsync upload execution failed.")

            except Exception as e:
                 if not self._handle_exception(e, attempt, remote_path, file_name):
                    raise
                 adaptive_timeout += 60
        
        raise RemoteTransferError(f"Rsync upload failed after {self.MAX_RETRY_ATTEMPTS}")
