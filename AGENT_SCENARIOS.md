# AGENT_SCENARIOS.md

This document provides practical examples of debugging steps and feature additions to guide AI agents and developers through complex tasks.

## Scenario 1: User Reports "Transfer Stuck at 95%"

**Symptom:** The UI shows a torrent at 95% (or similar) and speed is 0 MB/s. The logs show no errors, but the process eventually dies or hangs indefinitely.

### Diagnostic Steps
1.  **Check Logs**: Look for "Watchdog is terminating the process". If present, the process was killed due to inactivity.
2.  **Verify Watchdog**: If the watchdog didn't kill it, is the process actually stuck in `rsync`? Use `ps aux | grep rsync` on the host.
3.  **Identify Phase**: Is it stuck *transferring* or *verifying*?
    *   **Transferring**: Likely a network issue or specific file stall.
    *   **Verifying/Checksumming**: The `rsync` process might be calculating checksums for a large file.

### Likely Causes
*   **Heartbeat Failure**: The `heartbeat_callback` in `process_runner.py` isn't being called during a long checksum phase.
*   **Stalled SSH**: The underlying SSH connection died, but the Python script hasn't realized it yet (TCP timeout).

### Fix Locations
*   **`process_runner.py`**: Ensure `heartbeat_callback` is invoked even when `rsync` output doesn't match the progress regex (e.g., during startup or finalization).
*   **`config.ini`**: Increase `watchdog_timeout` for very large files.

### How to Test
*   Simulate a stall by adding `time.sleep(60)` in the transfer loop of `transfer_manager.py`.
*   Verify that the Watchdog kills it after the configured timeout.

## Scenario 2: Adding Support for a New qBittorrent API Feature

**Goal:** Add support for setting "Tags" on torrents after they are moved.

### Step-by-Step Implementation

1.  **Update `qbittorrent_manager.py`**:
    *   Modify `connect_qbit` if new client privileges are needed (unlikely for tags).
    *   Create a new function `add_tags(client, torrent_hash, tags)` that calls `client.torrents_add_tags(tags=tags, torrent_hashes=torrent_hash)`.

2.  **Update Configuration (`config.ini.template`)**:
    *   Add a new setting in `[SETTINGS]`:
        ```ini
        # (Optional) Tags to add to the torrent after moving (comma-separated).
        post_move_tags = moved, automated
        ```

3.  **Update Orchestrator (`torrent_mover.py`)**:
    *   In the main processing loop, after `transfer_manager` returns success:
        ```python
        tags = config['SETTINGS'].get('post_move_tags')
        if tags:
            qbittorrent_manager.add_tags(client, torrent_hash, tags)
        ```

4.  **Test**:
    *   Mock the `qbittorrentapi.Client` in a test case.
    *   Verify that `torrents_add_tags` is called with the correct arguments when a transfer completes successfully.
