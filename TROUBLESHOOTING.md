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
