# Torrent Mover

A Python script to automatically move completed torrents from a source qBittorrent client to a destination, with flexible and secure transfer options.

## Versioning

This project follows a `MAJOR.MINOR.PATCH` versioning scheme:

*   **MAJOR**: Incremented for significant, backward-incompatible changes.
*   **MINOR**: Incremented when new, backward-compatible functionality is added.
*   **PATCH**: Incremented for backward-compatible bug fixes or minor updates.

The current version is **2.7.3**. To check your version, run: `python3 -m torrent_mover.torrent_mover --version`.

## Changelog

### Version 2.7.4 (Latest)
* **fix(transfer)**: Fixed `rsync_upload` mode.
    * Resolved a `NameError` for `SSH_CONTROL_PATH` by removing a redundant local function in `transfer_manager.py`.
    * Completely rewrote the `transfer_content_rsync_upload` function to correctly execute `rsync` from the local machine, fixing `sshpass: command not found` errors.

### Version 2.7.3
* **fix(main)**: Fixed hang in `--simple` mode by refactoring all console logging logic. The script now correctly adds *only* a `StreamHandler` in simple mode and *only* a `RichHandler` in rich mode, resolving the `screen` conflict.

### Version 2.7.2
* **fix(ui)**: Implemented all stubbed methods in `SimpleUIManager` to correctly log progress to `stdout`, fixing the issue where `--simple` mode produced no output.

### Version 2.7.1
* **fix(main)**: Corrected a `NameError` for `UIManager` by updating function definition type hints to use the `BaseUIManager` interface.

### Version 2.7.0
* **feat(ui)**: Added a `--simple` flag to use a basic, non-interactive logging UI.
* **feat(ui)**: The script now detects if it's running in `gnu screen` and will warn the user, recommending they use the `--simple` flag to avoid UI corruption.
* **refactor(ui)**: Created a `BaseUIManager` interface to support both the "rich" (`UIManagerV2`) and new `SimpleUIManager` modes.

### Version 2.6.2 (Latest)
* **fix(ui)**: Corrected flawed logic in the `_stats_updater` thread that caused DL/UL speeds to reset to 0. Re-added `else` blocks to ensure the last known speed is used between 1-second update intervals.

### Version 2.6.1
* **fix(ui)**: Resolved a threading deadlock that caused DL/UL speeds in the "Overall Progress" bar to remain at 0.00 MB/s. The `progress.update()` call was moved outside the main UI lock.

### Version 2.6.0
* **feat(ui)**: Changed layout to 3/4 (Torrents) and 1/4 (Stats) based on user feedback.
* **feat(ui)**: Added a visual progress bar next to each active torrent.
* **feat(ui)**: Added visual progress bars for individual active files (requires data from `v2.5.12` backend).
* **fix(ui)**: Changed "Downloading" file status color from red to blue.
* **fix(ui)**: Changed "DL Speed" color in Stats panel from red to green.
* **fix(ui)**: Fixed a bug where DL/UL speed text would render incorrectly (`DL:[gre...`) in the main progress bar.

### Version 2.5.12
* **fix(ui)**: Corrected a flawed fix that caused an `AttributeError: 'function' object has no attribute 'format'`.
* **fix(ui)**: Replaced the incorrect `TextColumn(lambda ...)` with a proper custom `ProgressColumn` (`_SpeedColumn`) to render DL/UL speeds. This is the correct way to use callables for `rich` progress bars and resolves the startup crash.
* **fix(ui)**: Applied the remaining fixes from the code review, including pointing `_ActiveTorrentsPanel` to `_file_status` and adding bounds checking to `complete_file_transfer`.

### Version 2.5.11
* **fix(ui)**: Applied multiple fixes to `ui.py` based on an independent review to resolve critical `AttributeError` crashes and data access issues.
* **fix(ui)**: Corrected `TextColumn` format strings to use callables (lambda functions) instead of unsupported method calls (`.get()`).
* **fix(ui)**: Fixed `_ActiveTorrentsPanel` to correctly access file status from `self.ui_manager._file_status` instead of the non-existent `torrent.get("files", {})`.
* **fix(ui)**: Added more robust task existence checks in `_stats_updater` and bounds checking in `complete_file_transfer`.

### Version 2.5.10
* **fix(ui)**: Corrected a `TypeError` by removing an invalid `extra_description` argument from `TextColumn` and reverting to the standard, thread-safe `task.fields` implementation.

### Version 2.5.9
* **fix(ui)**: Refactored DL/UL speed display mechanism to avoid `task.fields` access during rendering, using direct attribute storage and column callables instead to prevent race conditions.

### Version 2.5.8
* **fix(ui)**: Refactored the entire UI rendering logic to be thread-safe, resolving a persistent crash. UI panels are now custom renderables, eliminating direct layout modification from background threads.

### Version 2.5.7
* **fix(ui)**: Implemented a more robust fix for Rich UI rendering errors by adding default fallbacks directly to DL/UL speed column formatters.
* **fix(ui)**: Ensured DL/UL byte counters are correctly initialized in stats.

### Version 2.5.6
* **fix(ui)**: Resolved a race condition that caused the Rich UI to crash on startup by providing default values for DL/UL speed fields.

### Version 2.5.5
* **feat(ui)**: Added separate Download (DL) and Upload (UL) speed indicators to the "Overall Progress" bar.
* **fix(ui)**: Prevented completed file count from exceeding total file count by adding checks in `complete_file_transfer`.
* **fix(log)**: Enhanced logging in transfer functions to explicitly record the filename and error details upon transfer failure.

### Version 2.5.4
* **fix(system)**: Corrected an `AttributeError` in `delete_destination_content` by importing the `Timeouts` class from `transfer_manager` instead of `ssh_manager`.

### Version 2.5.3
* **refactor(ui)**: Rearranged layout panels: Moved "Active Torrents" to the left column and "Recent Completions" to the middle column based on user feedback.

### Version 2.5.2
* **fix(transfer)**: Correct indentation of `try...except` blocks in `_sftp_download_file` to resolve a critical `SyntaxError`.

### Version 2.5.1
* **fix(main)**: Correctly capture transfer success/failure status from `_execute_transfer` to prevent rechecks on partial or failed transfers.
* **fix(transfer)**: Correct indentation of `try...except` blocks in `_sftp_upload_file` to resolve a `SyntaxError`.

### Version 2.5.0
* **feat(ui)**: Overhauled the "Active Queue" panel to display a detailed, color-coded file list (completed, downloading, uploading, queued) for each active torrent.
* **refactor(ui)**: Removed the redundant "Current Torrent" progress bar and the separate "Active Files" panel to create a cleaner, more integrated layout.

### Version 2.4.2
* **fix(main)**: Correct arguments in transfer_torrent call.

### Version 2.4.1
* **fix(main)**: Add missing qbittorrentapi import.

### Version 2.4.0
* **feat(main)**: Implement pre-transfer destination size check.

### Version 2.3.3
* **fix(main)**: Correct is_remote_dir call and enhance debug logs.

### Version 2.3.2
* **fix(main)**: Remove calls to non-existent `increment_failed` and `increment_completed` UI methods.

### Version 2.3.1
* **fix(main)**: Remove calls to non-existent UI methods.

### Version 2.3.0
* **feat**: Implement detailed failure reporting to the UI, including reasons like "Destination path already exists."

### Version 2.2.0
* **feat(ui)**: Add torrent name prefix to active files list.

### Version 2.1.0
* **feat**: Enhance TransferCheckpoint to track recheck failures.

### Version 2.0.0
* **Fix (UI):** Removed the `Panel` wrapper causing a hardcoded background color, allowing the UI to use the terminal's default background.

### Version 2.0.0-rc2
* **Fix (UI):** Correctly hide analysis progress bar after completion.

### Version 2.0.0-rc1
* **Feature (UI):** Overhauled the UI with a new multi-column layout for better information density.
* **Feature (UI):** Added a "Live Log" panel to display real-time logging output directly in the UI.
* **Feature (UI):** Added "Files Left" (e.g., `2/5 files`) to the "Active Queue" panel.
* **Feature (UI):** Added "Transferred" and "Remaining" size (e.g., `0.5 / 5.2 GB`) to the "Statistics" panel.
* **Feature (UI):** Added the current `transfer_mode` (e.g., `SFTP_UPLOAD`) to the header.
* **Feature (Resume):** Implemented robust file transfer resume logic. The script now checks if a local file exists (`os.path.exists`) before resuming, preventing errors from deleted cache files.
* **Fix (UI):** Corrected panel background colors for a consistent dark theme.
* **Fix (UI):** Added "Downloaded / Total" size (e.g., `100.2 MiB / 1.2 GiB`) to the "Active Files" panel.
* **Fix (UI):** Renamed the "Current" progress bar to "Current Torrent" for clarity.
* **Fix (UI):** Corrected header markup (e.g., `[dim]`) rendering as plain text instead of being styled.
* **Fix (Logic):** Resolved a major bug where "Overall Progress" and "Statistics" could show different total sizes.
* **Fix (Logic):** Corrected a bug where `local_cache_sftp_upload` mode used an incorrect total size for the "Current Torrent" progress bar.
* **Fix (Logic):** Improved the accuracy of the "Avg Speed" calculation.
* **Fix (Code):** Resolved an `AttributeError` for `update_footer` by implementing `set_final_status`.

### Version 1.9.2
*   **Fix**: Corrected an `IndentationError` in the `UIManagerV2` class that occurred after replacing the UI module.

### Version 1.9.1
*   **Feature**: Implemented a `FileTransferTracker` to checkpoint and resume individual file transfers, preventing restarts from scratch after failures.
*   **Feature**: Centralized all script timeouts into a `Timeouts` class, allowing users to configure them via environment variables (`TM_SSH_CONNECT_TIMEOUT`, `TM_RECHECK_TIMEOUT`, etc.) for greater flexibility.
*   **Feature**: Enhanced bandwidth monitoring in the UI to show the transfer speed of each individual active file, providing more granular insight into performance.
*   **Fix**: Consolidated performance logging to provide a clear, single-line summary of a file transfer's duration and average speed at the `DEBUG` level.

### Version 1.9.0
* **Feature**: Replaced the entire UI with a new high-performance version.
    * Uses a persistent `rich.layout.Layout` for a stable, flicker-free display.
    * Implements a live stats panel, removing the need for `display_stats()` at the end.
    * Dramatically reduces memory usage by using shared `Progress` objects instead of one per-file.
    * Shows only the last 5 active file transfers to prevent visual clutter on large torrents.

### Version 1.8.2
* **Fix**: Resolved a race condition in the `SSHConnectionPool` that could cause threads to block unnecessarily when the pool contained dead connections.
* **Fix**: Hardened the `UIManager` to prevent resource leaks by ensuring all progress bars are stopped on exit, even during a crash.
* **Fix**: Added dynamic batch sizing to remote size calculation (`du -sb`) to prevent shell command length limit errors on torrents with thousands of files.

### Version 1.8.1
*   **Fix**: Add robust handling for destination recheck failures. If a file transfer succeeds but the torrent fails the recheck on the destination, the script now assumes the data is corrupt and deletes it to force a full re-transfer on the next run.

### Version 1.8.0
*   **Feat**: The SFTP read/write chunk size is now configurable in `config.ini` via the `sftp_chunk_size_kb` setting. This allows users to tune the chunk size for their network conditions, which can lead to significant performance improvements, especially on high-latency networks. The default is `64` KB.

### Version 1.7.2
*   **Refactor**: The UI version is now set dynamically from the canonical `__version__` variable in `torrent_mover.py`, eliminating the hardcoded version in `ui.py`.
*   **UI Fix**: Corrected a crash caused by an improper `rich` library implementation for fixed-width columns. The per-file progress bars are now correctly aligned and no longer cause the application to hang.
*   **UI Enhancement**: The per-file progress display now includes the completed size vs. the total size (e.g., `50.1 MB / 100.2 MB`).

### Version 1.7.1
*   **UI Fix**: Updated the overall and torrent-level progress bars to display both the transferred size and the total size (e.g., `1.2 GB / 16.3 GB`) for better clarity, instead of only showing the total size.

### Version 1.7.0
*   **Security**: Applied `shlex.quote()` to all shell command constructions to prevent command injection vulnerabilities.
*   **Enhancement**: The `--version` output now includes the path to the configuration file being used.
*   **Feature**: Added a `--check-config` flag to validate the configuration file without running a full transfer.
*   **Logging**: The script now logs the path of the configuration file being used at startup.
*   **UI**: Verified that the transfer speed is correctly displayed in the final summary.
*   **Development**: Added a `requirements-dev.txt` file for testing tools.

### Version 1.6.3
*   **New Feature**: Introduced a Rich-based UI for real-time progress tracking of torrent analysis and transfers.
*   **New Feature**: Implemented local caching for `sftp_upload` mode to improve reliability and performance.
*   **Improvement**: Switched to using `du -sb` for remote size calculation, significantly speeding up the analysis phase.
*   **Improvement**: Added a destination health check to verify disk space and permissions before starting transfers.
*   **Improvement**: Added cleanup for orphaned cache directories from previous runs.

### Version 1.3.1
*   **Fix**: Implemented a robust, per-server SSH session throttling mechanism to prevent `Secsh channel open FAILED` errors. This resolves issues where analyzing or transferring many torrents at once could exhaust the server's available SSH session slots.
*   **New Config Option**: Added `max_concurrent_ssh_sessions` to each server block in `config.ini.template`, allowing granular control over connection limits for both source and destination servers.

## Features

*   **Works with Two qBittorrent Clients**: Manages torrents on both a source and a destination client via their WebUIs.
*   **Multiple Transfer Modes**:
    *   **`sftp`**: (Default) Securely downloads files from the source to the local machine running the script.
    *   **`sftp_upload`**: Securely transfers files directly from a source SFTP server to a destination SFTP server, bypassing the local machine.
    *   **`rsync_upload`**: Transfers files directly from the source server to the destination server via rsync. This bypasses the local machine and is often faster than sftp_upload.
    *   **`rsync`**: Uses `rsync` for potentially faster transfers from the source to the local machine.
*   **Concurrent Transfers**: Downloads or uploads multiple files in parallel to maximize transfer speed. The level of concurrency is configurable.
*   **Category-Based Moving**: Only moves torrents assigned to a specific category you define (e.g., "move").
*   **Fully Automated Process**:
    1.  Pauses the torrent on the source client.
    2.  Transfers the data to the destination.
    3.  Adds the torrent to the destination client (paused).
    4.  Triggers a force recheck to verify data integrity.
    5.  Starts the torrent on the destination for seeding.
    6.  Deletes the torrent and its data from the source to free up space.
*   **Intelligent Path Handling**: Automatically creates a parent directory for single-file torrents to ensure successful re-checking.
*   **Safe Testing Modes**: Includes `--dry-run` and `--test-run` flags to simulate the process without making permanent changes.
*   **Permission Testing**: A `--test-permissions` flag helps diagnose write-permission issues on the local or remote destination.
*   **Flexible Configuration**: All settings are managed in a simple `config.ini` file.
*   **Advanced Categorization**: Optionally, automatically assign categories on your destination client based on torrent trackers.
*   **Robust Error Handling**: If a torrent fails the recheck after transfer, its data on the destination is deleted, and the torrent is marked internally to prevent automatic retries on subsequent runs. This usually indicates a persistent issue (e.g., disk corruption, permission problems after transfer, or a qBittorrent bug) requiring manual investigation.

## Requirements

*   Python 3.6+
*   Two qBittorrent clients accessible over the network.
*   SSH/SFTP access to the source server (and destination server if using `sftp_upload` mode).

## Installation & Setup

It's highly recommended to run this script in a Python virtual environment.

### 1. Get the Code and Prepare the Environment

First, clone the repository. The script is designed to be run as a package, so you should be in the parent directory of the `torrent_mover` folder when running commands.

```bash
# Example: Clone into the /opt directory
cd /opt
git clone https://github.com/your-username/torrent-mover.git

# Navigate to the root of the cloned repository
cd torrent-mover
```

Now, create and activate a Python virtual environment from the repository root.

```bash
# Create the virtual environment
python3 -m venv .venv

# Activate it (you'll need to do this every time you open a new shell)
source .venv/bin/activate
```

### 2. Install Dependencies

With your virtual environment active, install the required Python libraries from `requirements.txt`.

```bash
pip install -r torrent_mover/requirements.txt
```

### 3. Install `sshpass` (Required for Rsync)

If you plan to use the `rsync` transfer mode, you must install `sshpass`.

*   **For Debian/Ubuntu:** `sudo apt-get update && sudo apt-get install sshpass`

**Note for `rsync_upload` mode**: This mode requires `rsync` and `sshpass` to be installed on your **source server**, as the script executes the transfer command remotely on that machine.
*   **For Fedora/CentOS/RHEL:** `sudo yum install sshpass`
*   **For Arch Linux:** `sudo pacman -S sshpass`

### 4. Create and Edit Your Configuration

The configuration files are located inside the `torrent_mover` directory.

```bash
# Navigate into the script's package directory
cd torrent_mover

# Copy the template to create your own config file
cp config.ini.template config.ini
```

Now, open `config.ini` with a text editor (like `nano` or `vi`) and fill in your server details. The template is organized into the following sections:

*   **`[SOURCE_CLIENT]` & `[DESTINATION_CLIENT]`**: Your qBittorrent client details.
*   **`[SOURCE_SERVER]`**: The SFTP/SSH details for your **source** server.
*   **`[DESTINATION_SERVER]`**: (Optional) The SFTP/SSH details for your **destination** server. **This is required only if `transfer_mode` is set to `sftp_upload`.**
*   **`[DESTINATION_PATHS]`**: The path on the destination where torrent data will be moved.
*   **`[SETTINGS]`**: Key operational settings, including:
    *   `transfer_mode`: `sftp`, `sftp_upload`, or `rsync`.
    *   `max_concurrent_file_transfers`: Number of files to transfer in parallel (e.g., `5`).
    *   `category_to_move`: The category in your source client that triggers a move.

## Basic Usage

**Important:** Because the script is now a package, it must be run with the `-m` flag from the **root directory of the repository** (the one containing the `.venv` and `torrent_mover` folders).

### Testing Your Setup (Recommended)

*   **Test Permissions**: Before the first run, verify that the script has the correct write permissions on the destination. It intelligently checks the local or remote path based on your `transfer_mode`.
    ```bash
    # From the repository root directory
    python3 -m torrent_mover.torrent_mover --test-permissions
    ```

*   **Dry Run (Simulation)**: This is the safest mode. It prints all the actions it *would* take without transferring files or changing any torrents.
    ```bash
    python3 -m torrent_mover.torrent_mover --dry-run
    ```

*   **Test Run (No Deletion)**: This performs a full run—transferring files and adding them to the destination—but **skips the final step of deleting the torrent from the source**.
    ```bash
    python3 -m torrent_mover.torrent_mover --test-run
    ```

### Normal Run

Once you've confirmed everything works, run the script normally:

```bash
# From the repository root directory
python3 -m torrent_mover.torrent_mover
```

## Scheduling with Cron

To run the script automatically, set up a cron job. **You must use absolute paths.**

1.  Open your crontab for editing: `crontab -e`
2.  Add a line to schedule the script. This example runs every 30 minutes.

```crontab
# Run the torrent mover every 30 minutes
# Note: The command must be run from the repository's root directory.
*/30 * * * * cd /opt/torrent-mover && .venv/bin/python3 -m torrent_mover.torrent_mover >> /opt/torrent-mover/cron.log 2>&1
```

**Breakdown of the command:**
*   `cd /opt/torrent-mover`: **Crucially**, first change to the repository's root directory.
*   `.venv/bin/python3`: Path to the Python executable in your virtual environment.
*   `-m torrent_mover.torrent_mover`: Runs the script as a module.
*   `>> .../cron.log 2>&1`: Appends all output to a log file for debugging.

## Command-Line Arguments

| Argument | Alias | Description |
|---|---|---|
| `--config [PATH]` | | Specifies the path to the `config.ini` file. |
| `--dry-run` | | Simulates the process without making any changes. |
| `--test-run` | | Performs a full run but **skips deleting torrents** from the source client. |
| `--debug` | | Enable debug logging to file. |
| `--simple` | | Use a simple, non-interactive UI. Recommended for `screen` or `tmux` to avoid visual glitches. |
| `--parallel-jobs [N]` | | Sets the number of *torrents* to process concurrently. Defaults to `4`. |
| `--test-permissions` | | Tests write permissions for the configured destination and exits. |
| `--categorize` | `-c` | Starts an interactive session to categorize torrents. |
| `--category [CATEGORY]` | | Overrides the default category for a `--categorize` session. |
| `--no-rules` | `-nr` | During a `--categorize` session, ignore existing rules. |
| `--list-rules` | `-l` | Lists all saved tracker-to-category rules and exits. |
| `--add-rule [DOMAIN] [CATEGORY]` | `-a` | Adds or updates a categorization rule and exits. |
| `--delete-rule [DOMAIN]` | `-d` | Deletes a specific categorization rule and exits. |
| `--clear-recheck-failure [TORRENT_HASH]` | | Manually removes a torrent from the internal 'recheck_failed' list, allowing the script to process it again. Use after resolving the underlying issue. |
| `--version` | | Displays the current version of the script and exits. |

## Advanced Usage: Tracker-Based Categorization

This script can automatically assign a category to torrents on your destination client based on their trackers. Rules are stored in `tracker_rules.json`.

*   **Interactive Learning**: The easiest way to create rules is with the interactive mode.
    ```bash
    # Start interactive mode
    python3 -m torrent_mover.torrent_mover -c
    ```
*   **Manual Rule Management**:
    *   **List**: `python3 -m torrent_mover.torrent_mover -l`
    *   **Add/Update**: `python3 -m torrent_mover.torrent_mover -a "some-tracker.org" "My-TV-Shows"`
    *   **Delete**: `python3 -m torrent_mover.torrent_mover -d "some-tracker.org"`
