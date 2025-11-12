# Torrent Mover

A Python script to automatically move completed torrents from a source qBittorrent client to a destination, with flexible and secure transfer options.

## Versioning

This project follows a `MAJOR.MINOR.PATCH` versioning scheme:

*   **MAJOR**: Incremented for significant, backward-incompatible changes.
*   **MINOR**: Incremented when new, backward-compatible functionality is added.
*   **PATCH**: Incremented for backward-compatible bug fixes or minor updates.

The current version is **2.9.0**. To check your version, run: `python3 -m torrent_mover.torrent_mover --version`.

## Changelog

### Version 2.9.0 (Latest)
* **feat(recheck):** Implemented a new 3-stage recheck and delta-transfer workflow to significantly improve the recovery rate of partially failed transfers. This new process intelligently retries transfers, deletes only the corrupted files, and performs multiple rechecks to ensure data integrity, reducing the need for manual intervention.

### Version 2.7.6
* **fix(transfer):** (Fix #1) Fixed a critical bug where paths from qBittorrent containing surrounding quotes (e.g., `'/path/to/file'`) would cause `rsync` transfers to fail due to malformed, double-escaped quotes.
* **fix(transfer):** (Fix #2) Resolved an `AttributeError: 'bool' object has no attribute 'is_corrupted'` by correcting a type hint error in `transfer_manager.py`, re-enabling corruption checks for rsync transfers.
* **fix(strategy):** (Fix #3) Updated transfer strategies to correctly preserve the parent folder structure for single-file torrents.
* **fix(rsync):** (Fix #4) Optimized rsync transfers by changing the default options to `-ahHSP --partial --inplace` to handle sparse files (`-S`), preserve hard links (`-H`), show progress (`-P`), and reduce verbosity (removed `-v`).
* **fix(ui):** (Fix #5) Corrected the UI speed display to reset to 0.00 MB/s after 5 seconds of inactivity, preventing stale speed data from persisting.
* **fix(log):** Silenced verbose `rsync` progress lines from spamming the `DEBUG` log for both `rsync` and `rsync_upload` modes.

### Older Versions (Summarized)
*   **v2.7.5**: fix(main): Fixed incorrect dependency check for `sshpass` and corrected `rsync_upload` logic.
*   **v2.7.4**: fix(transfer): Fixed `rsync_upload` mode by resolving a `NameError` and rewriting the transfer function.
*   **v2.7.3**: fix(main): Fixed hang in `--simple` mode by refactoring console logging.
*   **v2.7.2**: fix(ui): Implemented stubbed methods in `SimpleUIManager` to fix `--simple` mode output.
*   **v2.7.1**: fix(main): Corrected a `NameError` for `UIManager`.
*   **v2.7.0**: feat(ui): Added a `--simple` flag for a non-interactive UI and detection for `gnu screen`.
*   **v2.6.2**: fix(ui): Corrected flawed logic in the `_stats_updater` thread that caused speeds to reset to 0.
*   **v2.6.1**: fix(ui): Resolved a threading deadlock that caused DL/UL speeds to remain at 0.00 MB/s.
*   **v2.6.0**: feat(ui): Changed layout, added visual progress bars, and improved UI colors.
*   **v2.5.0 - v2.5.12**: This series of releases focused heavily on UI stability and features, including a major overhaul of the "Active Queue" panel, a thread-safe rendering logic, and fixes for multiple `AttributeError` and `TypeError` crashes.
*   **v2.0.0 - v2.4.2**: Introduced a new multi-column UI with a live log panel, robust file transfer resume logic, pre-transfer destination size checks, and detailed failure reporting.
*   **v1.3.1 - v1.9.2**: These initial versions introduced the first Rich-based UI, local caching for `sftp_upload` mode, per-server SSH session throttling, configurable SFTP chunk sizes, and numerous improvements to performance and reliability.

## Features

*   **Works with Two qBittorrent Clients**: Manages torrents on both a source and a destination client via their WebUIs.
*   **Multiple Transfer Modes**:
    *   **`sftp`**: (Default) Securely downloads files from the source to the local machine running the script.
    *   **`sftp_upload`**: Securely transfers files directly from a source SFTP server to a destination SFTP server, bypassing the local machine.
    *   **`rsync_upload`**: Transfers files by first downloading from the source server to a **local cache** (on the machine running the script) via rsync, and then uploading from the cache to the destination server via rsync. This is useful for high-speed transfers and avoids needing sshpass on the source server.
    *   **`rsync`**: Uses `rsync` for potentially faster transfers from the source to the local machine. It now fully supports real-time transfer progress in the UI and automatically resumes partial transfers (if a file exists with a different size) instead of deleting and re-transferring.
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
*   **Resilient Transfers**: Includes a resilient transfer queue with a circuit breaker to automatically retry failed transfers with exponential backoff.
*   **Robust Resume**: Provides robust resume capabilities for interrupted transfers, checkpointing progress at the individual file level.
*   **Robust 3-Stage Recheck**: If a recheck fails, the script will (1) attempt a delta-transfer and recheck again. If it still fails, it will (2) delete only the specific bad files, re-download them, and recheck a final time. This significantly improves reliability for rsync and sftp transfers.

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

**Note for `rsync` and `rsync_upload` modes**: Both modes require `rsync` and `sshpass` to be installed on the **local machine** (the one running this script).
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
    *   `max_concurrent_downloads`: Number of files to download in parallel (e.g., `4`).
    *   `max_concurrent_uploads`: Number of files to upload in parallel (e.g., `4`).
    *   `category_to_move`: The category in your source client that triggers a move.
    *   `pool_wait_timeout`: (Optional) Time in seconds to wait for a connection from the SSH pool if it's full. Defaults to `300`. Increase this if you get `TimeoutError` logs.
    *   `allow_near_complete_rsync`: (Optional) For rsync transfers, allows a recheck to pass at 99.9%+ completion. This is useful for minor metadata differences that rsync might not sync.

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
| `--clear-corruption [TORRENT_HASH]`    |      | Manually removes corruption markers for a specific torrent, allowing a failed transfer to be retried from scratch. |
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
