# CHANGELOG-AGENT.md

This log tracks architectural changes and refactors that significantly affect how an AI agent should understand and modify the codebase.

## 2.13.1 - Progress Parsing and UI Fixes

**Summary:** Refactored `process_runner.py` to directly parse `rsync --info=progress2` output for more accurate progress tracking. Corrected Peak Speed calculation logic in `UIManagerV2` and improved Statistics panel layout.

**Impact on AI:**
*   **Progress Parsing:** The system now handles `rsync` progress output more robustly, resolving discrepancies between the UI progress bar and the actual transfer status.
*   **UI Logic:** Peak Speed calculation is now accurate.
*   **Layout:** Statistics panel and Network Graph visibility have been improved.

## Unreleased
 
## 2.14.0 - Permissions, Relay Mode, and Checks

**Summary:** Fixed Permission Denied loops by implementing Pre-Flight Unlock (`chmod 777`) and disabling rsync metadata preservation. Refactored rsync_upload mode to 'Relay Mode'. Added local_cache_path configuration, pre-flight storage capacity checks, and weighted progress bars (50% DL / 50% UL) for smoother UI feedback.

**Impact on AI:**
*   **Permission Fixes:** The system now enforces `chmod 777` on destination before transfer and strips metadata flags from rsync.
*   **Rsync Relay Mode:** `rsync_upload` now strictly follows a Download -> Cache -> Upload workflow.
*   **Storage Checks:** The system now enforces storage capacity checks before starting transfers. Ensure `local_cache_path` is configured for large transfers.
*   **Progress Reporting:** Progress for `rsync_upload` is split: 0-50% for download, 50-100% for upload.

## 2024-01-15 - Delta Progress Calculation Refactor

**Summary:** Progress reporting logic in `process_runner.py` and `ui.py` was refactored to strictly use **delta** updates instead of absolute values.

**Impact on AI:**
*   **Do not** set absolute progress values (e.g., `progress_bar.update(100)`). This will cause the total transferred bytes to skyrocket and the percentage to exceed 100%.
*   **Always** calculate the difference (`delta = current - last`) and pass that to `ui.update_torrent_progress`.
*   Old methods that set absolute progress directly are deprecated and will break the stats display.

## 2024-01-10 - Recheck Failure States Split

**Summary:** The logic for handling failed rechecks in `qbittorrent_manager.py` was split into two distinct states: `FAILED_STUCK` and `FAILED_FINAL_REVIEW`.

**Impact on AI:**
*   **`FAILED_STUCK`**: Indicates the recheck process hung (0% progress for `recheck_stuck_timeout`). The system *should* attempt a delta-sync repair.
*   **`FAILED_FINAL_REVIEW`**: Indicates the recheck completed (or stopped) but the data is corrupt (progress < 100%). The system *should* attempt a delta-sync repair.
*   This distinction allows for more granular timeouts and better logging. When debugging recheck issues, check which of these two states triggered the failure.

## 2024-01-24 - Smart Heartbeat Verification

**Summary:** The "Smart Heartbeat" logic in `process_runner.py` was verified using `tests/test_process_runner_smart.py`.

**Impact on AI:**
*   The `execute_streaming_command` function is confirmed to handle slow streams (using `heartbeat_callback`) and complete hangs (using `timeout_seconds`).
*   Tests `test_smart_progress_success` and `test_smart_progress_timeout` are the canonical references for this behavior.
