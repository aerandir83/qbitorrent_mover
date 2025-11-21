# CRITICAL_PATHS.md

This document maps common modification scenarios to the exact code locations that require changes. It serves as a quick-reference guide for developers and AI agents to identify the "critical path" for specific tasks.

## Adding a New Transfer Mode

To add a new transfer mode (e.g., `ftp`, `rclone`), you must modify the following files in this order:

1.  **`transfer_strategies.py`**:
    *   **Define the Strategy Class**: Create a new class inheriting from `TransferStrategy` (e.g., `FTPStrategy`).
    *   **Implement Abstract Methods**: Implement `prepare_files`, `supports_parallel`, and `supports_delta_correction`.
    *   **Register the Strategy**: Update the `get_transfer_strategy` factory function to include your new mode string and class mapping.

2.  **`transfer_manager.py`**:
    *   **Execution Logic**: If your new mode requires a unique execution flow (like `rsync_upload` vs `sftp`), add a dedicated function (e.g., `transfer_content_ftp`).
    *   **Integration**: Update `TorrentMover.process_torrent` (or the relevant caller) to route to your new execution function based on the mode.

3.  **`config.ini.template`**:
    *   **New Parameters**: Add any new configuration settings required for your mode (e.g., `ftp_timeout`).
    *   **Documentation**: Update the `transfer_mode` comment to list the new option.

4.  **`tests/`**:
    *   Add a new test file (e.g., `test_transfer_strategies_ftp.py`) to verify the file preparation logic.

## Modifying Progress Reporting

Progress reporting flows from the low-level process runners up to the UI. Changes here are delicate due to potential race conditions and UI flickering.

1.  **`ui.py`**:
    *   **`UIManagerV2`**: This is the central hub. Modify `_stats_updater` for new metrics or `update_torrent_progress` for logic changes.
    *   **Renderables**: Modify `_StatsPanel`, `_ActiveTorrentsPanel`, or `_SpeedColumn` to change *how* data is displayed.
    *   **Layout**: `ResponsiveLayout` controls what is shown based on terminal width.

2.  **`process_runner.py`**:
    *   **`execute_streaming_command`**: This function parses raw stdout (e.g., from rsync).
    *   **Regex Parsing**: Modify `progress_regex` if the output format of the underlying tool changes.
    *   **Delta Calculation**: Ensure `last_transferred_bytes` logic correctly calculates the delta to prevent double-counting.

3.  **`transfer_manager.py`**:
    *   **`RateLimitedFile`**: Used for SFTP progress. Ensure it calls `ui.update_torrent_progress` correctly.
    *   **Orchestration functions**: ensure total sizes are passed correctly to initialization methods.

**Common Pitfalls:**
*   **>100% Progress**: Usually caused by double-counting bytes in `process_runner.py` or not resetting accumulators on retry.
*   **Delta Errors**: The UI expects *delta* updates (bytes transferred *since last update*), not absolute totals.

## Fixing a Stuck Transfer Bug

If transfers are stalling, check these critical components:

### Diagnostic Checklist
1.  **Watchdog (`watchdog.py`)**: Is it triggering? The watchdog kills the process if `ui.pet_watchdog()` isn't called for `watchdog_timeout` seconds.
2.  **Heartbeat**: In `process_runner.py`, `heartbeat_callback` ensures the watchdog is "petted" even during long rsync operations without progress output.
3.  **SSH Keepalive**: In `ssh_manager.py`, check `ServerAliveInterval` and `ServerAliveCountMax` settings in `_get_ssh_command`.

### Common Causes
*   **Checksum Phase**: Large files take a long time to checksum. `process_runner.py` must receive heartbeats during this phase, or the watchdog will time out.
*   **Firewall/Network**: Silent drops of SSH connections. The `ResilientTransferQueue` in `transfer_manager.py` handles retries, but persistent failures will eventually stop the worker.
*   **Output Buffering**: `process_runner.py` uses `stdbuf -o0` to force unbuffered output. Ensure this is working on the host system.
