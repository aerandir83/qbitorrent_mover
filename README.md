# Torrent Mover

A Python script to automatically move completed torrents from a source qBittorrent client to a destination, with flexible and secure transfer options.

## Versioning

This project follows a `MAJOR.MINOR.PATCH` versioning scheme:

*   **MAJOR**: Incremented for significant, backward-incompatible changes.
*   **MINOR**: Incremented when new, backward-compatible functionality is added.
*   **PATCH**: Incremented for backward-compatible bug fixes or minor updates.

The current version is **2.0.0-rc1**. To check your version, run: `python3 -m torrent_mover.torrent_mover --version`.

## Changelog

### Version 2.0.0-rc1 (Latest)
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
| `--parallel-jobs [N]` | | Sets the number of *torrents* to process concurrently. Defaults to `4`. |
| `--test-permissions` | | Tests write permissions for the configured destination and exits. |
| `--categorize` | `-c` | Starts an interactive session to categorize torrents. |
| `--category [CATEGORY]` | | Overrides the default category for a `--categorize` session. |
| `--no-rules` | `-nr` | During a `--categorize` session, ignore existing rules. |
| `--list-rules` | `-l` | Lists all saved tracker-to-category rules and exits. |
| `--add-rule [DOMAIN] [CATEGORY]` | `-a` | Adds or updates a categorization rule and exits. |
| `--delete-rule [DOMAIN]` | `-d` | Deletes a specific categorization rule and exits. |
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