# System Invariants ("Unbreakable Rules")

This document defines the invariants that must hold true at all times. Violating these invariants is considered a critical system failure.

## Mandatory Invariants

1. **Transfer progress must never exceed 100%.**
    * **Enforcement Locations:**
        * `torrent_mover.py:_update_transfer_progress()`
        * `ui.py:_ActiveTorrentsPanel.__rich_console__()`
2. **Source deletion requires `transfer_success == TRUE` AND `chown_success == TRUE`.**
    * **Enforcement Locations:**
        * `torrent_mover.py:_post_transfer_actions` (guarded by `if post_transfer_success`)
3. **Checkpoint writes are atomic.**
4. **A torrent is eligible for processing if and only if its download progress is 100% (`progress == 1`).**
