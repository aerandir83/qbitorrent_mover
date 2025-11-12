# Torrent Mover Workflow

This document details the high-level logical flow of the Torrent Mover application, from initialization to the completion of a torrent transfer.

## 1. Initialization and Setup

The application's lifecycle begins in `torrent_mover.py`'s `main()` function.

1.  **Argument Parsing**: Parses command-line arguments (e.g., `--dry-run`, `--debug`, `--simple`).
2.  **Config Loading & Update**:
    * `config_manager.update_config()` is called to check the user's `config.ini` against `config.ini.template`.
    * Any new options from the template are added to the user's `config.ini` non-destructively, preserving their existing values. A backup is created if changes are made.
    * `config_manager.load_config()` loads the finalized configuration into a `ConfigParser` object.
3.  **Config Validation**:
    * `config_manager.ConfigValidator` runs to ensure all required sections (e.g., `[SOURCE_CLIENT]`, `[SETTINGS]`) and options are present and valid.
    * It checks for valid `transfer_mode` values and mode-specific requirements (e.g., `rsync_upload` requires `[DESTINATION_SERVER]`).
    * The script exits if any critical errors are found.
4.  **Logging Setup**:
    * `system_manager.setup_logging()` configures file-based logging (e.g., `logs/torrent_mover_YYYY-MM-DD.log`).
    * The appropriate UI manager (`UIManagerV2` for rich UI or `SimpleUIManager` for `--simple` mode) is initialized and its logging handler is attached.
5.  **Singleton Lock**:
    * `system_manager.LockFile.acquire()` is called to create a `.torrent_mover.lock` file containing the script's PID.
    * This prevents multiple instances from running simultaneously. The lock is automatically released on exit.
6.  **Dependency Checks**:
    * For `rsync` modes, `ssh_manager.check_sshpass_installed()` verifies that `sshpass` is in the system's PATH.
    * SSH multiplexing paths are prepared by `ssh_manager.setup_ssh_control_path()` to speed up `rsync` operations.

## 2. Utility Command Handling

The script checks if a "utility command" (like `--list-rules` or `--test-permissions`) was passed.
* If so, it executes that specific action (e.g., `tracker_manager.display_tracker_rules()` or `system_manager.test_path_permissions()`) and exits immediately without starting the main transfer process.

## 3. Main Transfer Operation (`_run_transfer_operation`)

If no utility command is given, the main operation begins.

1.  **Client Connections**:
    * Connects to the Source and Destination qBittorrent clients using `qbittorrent_manager.connect_qbit()`.
    * Initializes `ssh_manager.SSHConnectionPool` for the Source and (if needed) Destination servers.
2.  **Cache Management**:
    * `system_manager.cleanup_orphaned_cache()` runs to find and delete any stale `torrent_mover_cache_*` directories from previous runs that correspond to torrents already completed on the destination.
    * `system_manager.recover_cached_torrents()` finds cache directories for torrents that are *not* on the destination, adding them to the processing queue to resume the interrupted transfer.
3.  **Watchdog Start**: The `watchdog.TransferWatchdog` is started in a background thread to monitor for stalled transfers and force-exit the script if no progress is made for a set timeout.
4.  **Torrent Identification**:
    * `qbittorrent_manager.get_eligible_torrents()` queries the source client.
    * It fetches all completed torrents in the `category_to_move`.
    * If `size_threshold_gb` is set, it intelligently selects only the oldest torrents needed to bring the category size below the threshold.
5.  **Pre-Transfer Health Check**:
    * If any torrents are found, `system_manager.destination_health_check()` is performed.
    * This function checks for:
        1.  **Disk Space**: Verifies the destination (local or remote) has enough free space for the total transfer size.
        2.  **Write Permissions**: Creates and deletes a temporary file to confirm write access.
    * The script exits if this check fails.
6.  **Transfer Execution (Parallel)**:
    * A `concurrent.futures.ThreadPoolExecutor` is used to process all eligible torrents in parallel (up to `parallel_jobs`).
    * Each torrent is processed by the `transfer_torrent()` function.

## 4. Single Torrent Workflow (`transfer_torrent`)

This function orchestrates the entire process for one torrent.

1.  **Pre-Transfer Setup (`_pre_transfer_setup`)**:
    * Resolves the final source and destination content paths.
    * Checks if content of the same name already exists at the destination (locally or remotely).
    * It compares the size of the source content with any existing destination content.
    * It returns a status:
        * `exists_same_size`: The torrent is skipped as it's already present.
        * `exists_different_size`: The transfer will proceed (rsync will resume, others will delete and re-transfer).
        * `not_exists`: A new transfer will proceed.
2.  **Transfer Execution**:
    * **Strategy Selection**: `transfer_strategies.get_transfer_strategy()` instantiates the correct strategy class (e.g., `SFTPStrategy`, `RsyncStrategy`) based on the `transfer_mode`.
    * **File Preparation**: The strategy's `.prepare_files()` method is called.
        * For `sftp` modes, this recursively lists all individual files.
        * For `rsync` modes, this lists only the top-level path.
    * **Transfer**: The corresponding function from `transfer_manager.py` is executed (e.g., `transfer_content_with_queue` for SFTP, `transfer_content_rsync` for rsync). These functions handle the actual data movement, progress reporting (via the UI), and resumability (via `FileTransferTracker`).
3.  **Post-Transfer Actions (`_post_transfer_actions`)**:
    * This function runs if the transfer was successful *or* if the torrent was skipped because it already existed.
    * **Change Ownership**: `system_manager.change_ownership()` is called if `chown_user` or `chown_group` is set.
    * **Add to Destination**: The `.torrent` file is added to the destination qBittorrent client in a paused state.
    * **Recheck & Recovery**: This is a robust, multi-stage process managed by `_handle_recheck_process`:
        1.  **Initial Recheck**: `qbittorrent_manager.wait_for_recheck_completion()` monitors the torrent.
        2.  **Failure (Stage 1)**: If recheck fails (e.g., 90% complete), the script gets a list of incomplete files from `qbittorrent_manager.get_incomplete_files()`.
        3.  **Delta Transfer**: The script performs a "delta transfer," re-transferring *only* the failed files.
        4.  **Second Recheck**: The torrent is rechecked again.
        5.  **Failure (Stage 2)**: If it fails again, `system_manager.delete_destination_files()` is called to delete *only* the bad files from the destination.
        6.  **Final Delta Transfer**: The bad files are re-transferred one last time.
        7.  **Final Recheck**: The torrent is rechecked a final time. If this fails, the entire process for this torrent fails, and it is marked in `transfer_checkpoint.json` to be skipped in the future.
    * **Finalization**: If the recheck is successful:
        1.  `tracker_manager.set_category_based_on_tracker()` applies any matching category rules.
        2.  The torrent is resumed on the destination client (if configured).
        3.  The torrent and its data are deleted from the source client (if configured and not in `--test-run`).

## 5. Shutdown

Once all torrents are processed, the main thread:
1.  Stops the `TransferWatchdog`.
2.  Closes all `SSHConnectionPool`s.
3.  Exits, which automatically triggers the `system_manager.LockFile.release()` to remove the lock file.
