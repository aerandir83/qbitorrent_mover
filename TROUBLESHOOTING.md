# Troubleshooting Guide

This guide helps diagnose and resolve common issues encountered during the operation of the Torrent Mover.

## Common Issues

### 1. Rsync Stalls

*   **Symptom:** The transfer appears to hang or stall, especially during the initial phase, with no progress updates.
*   **Root Cause:** Rsync is performing a checksum calculation on large files, which can take a significant amount of time without transmitting data, leading to potential timeouts.
*   **Solution:** The "Smart Heartbeat" mechanism is implemented to detect activity during the checksum phase and prevent the system from treating it as a stall. Ensure Smart Heartbeat is enabled and functioning.
*   **Related Code:** `SSHManager`, `watchdog.py` (Smart Heartbeat logic).

### 2. Progress > 100%

*   **Symptom:** The transfer progress bar displays a value greater than 100%.
*   **Root Cause:** Errors in delta calculation or estimation when files are being modified or when the `Transfer Multiplier` is applied incorrectly can lead to progress values exceeding the total size.
*   **Solution:** Implement logic to clamp progress values between 0% and 100%. Ensure that reported progress never exceeds the maximum possible value.
*   **Related Code:** `TransferManager`, `UIManager` (Progress clamping logic).

### 3. Permission Errors

*   **Symptom:** Transfers fail with "Permission denied" errors or inability to write to the destination directory.
*   **Root Cause:** The user running the script does not have the necessary read/write permissions on the source or destination directories.
*   **Solution:** Verify permissions using the `--test-permissions` flag. Ensure the user has the correct ownership and mode bits set on the target paths.
*   **Related Code:** `torrent_mover.py` (Main entry point), `SystemManager` (`test_path_permissions` check).

### 4. Rsync Permission Denied / 'nobody' ownership issues

*   **Symptom:** Transfers fail with "Permission denied" errors or inability to write to the destination directory.
*   **Explanation:** This occurs when the destination files are owned by `nobody` (common in Unraid/Docker) and the local user cannot overwrite them.
*   **Solution:** The script now automatically performs a 'Pre-Flight Unlock' (chmod 777) via SSH before transfer. Ensure `DESTINATION_SERVER` is configured with a user that has `sudo` or ownership rights (or that the files are essentially public).

### 5. SFTP Timeouts / Connection Failures

*   **Symptom:** Logs show `TimeoutError`, `EOFError`, or `Secsh channel open FAILED`.
*   **Root Cause:** Network instability, firewall dropping idle connections, or exceeding the SSH server's `MaxSessions` limit.
*   **Solution:** 
    *   Increase `pool_wait_timeout` in `config.ini` if parallel transfers are contending for connections.
    *   Reduce `max_concurrent_ssh_sessions` in `config.ini`.
    *   Check `watchdog_timeout` if large files are timing out during checksums.

### 6. Storage Capacity Errors

*   **Symptom:** Transfers fail to start with a "Insufficient disk space" error.
*   **Root Cause:** The destination (or local cache for `rsync_upload`) does not have enough free space to hold the torrent.
*   **Solution:**
    *   Free up space on the destination drive.
    *   For `rsync_upload` (Relay Mode), configure `local_cache_path` in `config.ini` to point to a drive with sufficient space, as it caches the entire torrent locally before uploading.

## Diagnostic Decision Trees

### Tree 1: 0% Progress Stalls

*   **Is the transfer mode `rsync`?**
    *   **YES:** Check `process_runner.py`.
        *   Verify regex parsing matches the rsync version's output.
        *   Check the "Smart Heartbeat" mechanism to ensure it detects activity.
    *   **NO (it is `sftp`):** Check `transfer_manager.py`.
        *   Verify that progress callbacks are being invoked.
        *   Check for potential deadlocks in the UI lock mechanism.

### Tree 2: Permission Denied Errors

*   **Is the error on the Destination?**
    *   **Local Destination:**
        *   Run with `--test-permissions` to verify the user has write access.
        *   Check directory ownership and mode bits (`chmod`/`chown`).
    *   **Remote Destination (SFTP/Rsync):**
        *   Check SSH access and key authentication.
        *   Verify the remote user has write permissions on the target path.
