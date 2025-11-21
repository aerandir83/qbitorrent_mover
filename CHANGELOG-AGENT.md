# CHANGELOG-AGENT.md

This log tracks architectural changes and refactors that significantly affect how an AI agent should understand and modify the codebase.

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
