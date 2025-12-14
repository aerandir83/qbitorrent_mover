# Torrent Mover

A Python script to automatically move completed torrents from a source qBittorrent client to a destination, with flexible and secure transfer options.

## Versioning

This project follows a `MAJOR.MINOR.PATCH` versioning scheme:

*   **MAJOR**: Incremented for significant, backward-incompatible changes.
*   **MINOR**: Incremented when new, backward-compatible functionality is added.
*   **PATCH**: Incremented for backward-compatible bug fixes or minor updates.

The current version is **2.14.0**. To check your version, run: `python3 torrent_mover.py --version`.

## Changelog

### Version 2.14.0 (Latest)
* **feat(resilience):** Implemented "Pre-Flight Unlock" to fix Permission Denied loops by running `chmod 777` on the destination before transfer.
* **feat(rsync):** Refactored `rsync_upload` to strict "Relay Mode" (Download -> Cache -> Upload) for better reliability.
* **feat(config):** Added `local_cache_path` configuration for `rsync_upload` mode, allowing use of a specific drive for large temporary files.
* **feat(checks):** Added pre-flight storage capacity checks to prevent filling up the disk.
* **feat(ui):** Added weighted progress bars for `rsync_upload` (50% Download / 50% Upload) for smoother feedback.

### Version 2.13.1
* **fix(progress):** Refactored rsync progress parsing to directly handle `--info=progress2` output, resolving UI vs Log discrepancies.
* **fix(ui):** Corrected "Peak Speed" calculation logic in the dashboard.
* **style(ui):** Improved the layout of the Statistics panel and increased Network Graph visibility.

### Version 2.12.0
* **feat(startup):** Refactored startup sequence with explicit "Pre-flight Checks" for connectivity and disk space before UI launch.
* **feat(filtering):** Implemented strict exclusion for torrents matching "Ignore" tracker rules.
* **feat(resilience):** Added distinction for "Provisioning Errors" vs general transfer failures.
* **docs**: Updated documentation to reflect architectural changes.

### Version 2.11.0
* **feat(resilience):** Added Smart Heartbeat monitoring, a configurable watchdog_timeout, and SSH KeepAlive to dramatically improve resilience against network stalls and long checksum operations.
* **docs**: Updated README and config template with information about the new features.

### Version 2.10.1
* **fix(subprocess):** Changed subprocess buffering from line-buffered (`bufsize=1`) to unbuffered (`bufsize=0`) in binary mode to prevent `RuntimeWarning` and ensure immediate byte-by-byte processing of `rsync` output, improving the responsiveness of the Heartbeat logic.

### Version 2.10.0
* **feat**: Expanded Unit Test Coverage: Added strict command verification and resilience mocking for rsync transfer mode.
* **docs**: Documentation Overhaul: Restructured README to focus on Workflows and Resilience narratives.
* **chore**: Housekeeping: Consolidated historical changelogs.

### Version 2.9.5
* **fix(rsync):** Updated the `rsync` progress parsing regex to be more robust. The new regex (`^\s*([\d,]+).*$`) correctly captures the byte count even if the line from `rsync --info=progress2` does not contain trailing whitespace, resolving failures in DL speed calculation.

### Version 2.9.1
* **feat(recheck):** Implemented a new 3-stage recheck and delta-transfer workflow to significantly improve the recovery rate of partially failed transfers. This new process intelligently retries transfers, deletes only the corrupted files, and performs multiple rechecks to ensure data integrity, reducing the need for manual intervention.

### Historical Summary
<details>
<summary>Click to expand</summary>

Older changelog entries have been consolidated into this section.

*   **v2.7.x-v2.8.x**: Focused on UI enhancements, including a `--simple` mode for non-interactive terminals, and numerous bug fixes for `rsync` and `sftp` transfer modes.
*   **v2.6.x**: Introduced a major UI overhaul with a multi-column layout, live log panel, and detailed progress bars.
*   **v2.5.x**: Added a resilient transfer queue with a circuit breaker and robust resume capabilities.
*   **v2.0.x-v2.4.x**: Implemented the core architecture, including the qBittorrent client managers, SSH/SFTP connection pool, and basic transfer strategies.
*   **v1.x.x**: Initial versions of the script, focused on basic SFTP transfers and cron-based scheduling.

</details>

## Core Workflows

This section outlines the primary workflows that Torrent Mover uses to transfer and manage data reliably.

### Transfer Workflows

The script offers several modes to transfer data, each suited for different server setups and performance needs:

*   **`sftp` (Download)**: The default mode. Securely downloads files from the source server to the local machine running the script using the SFTP protocol. This is ideal for simple setups where you want to centralize downloads.
*   **`sftp_upload` (Cache-to-Remote)**: Securely transfers files directly from a source SFTP server to a destination SFTP server. The machine running the script orchestrates the transfer, downloading to a local cache and then uploading to the remote destination. This is useful when the source and destination servers are remote.
*   **`rsync_upload` (Relay Sync)**: Uses the machine running the script as a bridge. Downloads data from Source using rsync (delta-sync) to a local cache, then uploads to Destination. Requires 2x bandwidth and local storage matching the torrent size. Recommended to configure `local_cache_path` for large torrents.
*   **`rsync` (Delta-Sync)**: Uses `rsync` for potentially faster transfers from the source to the local machine. `rsync` is highly efficient as it only transfers the differences between files (delta synchronization), making it ideal for resuming interrupted transfers or syncing large files.

### Resilience & Self-Healing

Torrent Mover is designed to handle failures gracefully and ensure data integrity.

*   **3-Stage Recheck Logic**: If a transferred torrent fails the initial integrity recheck on the destination client, the script automatically triggers a 3-stage recovery process:
    1.  **Delta-Sync**: It first attempts an `rsync` delta-transfer to fix any corrupted or missing pieces and rechecks again.
    2.  **Targeted Deletion**: If that fails, it queries the qBittorrent client for the list of bad files, deletes *only* those specific files, and re-downloads them.
    3.  **Final Recheck**: A final recheck is performed to ensure the torrent is 100% complete.
*   **Watchdog & Smart Heartbeat**: A built-in watchdog monitors for hung transfers. It now uses "Smart Heartbeat" monitoring, which intelligently detects signs of life from `rsync` even during long checksum phases. If a transfer is truly stalled for an extended period (configurable via `watchdog_timeout`), it is terminated and requeued.
*   **SSH KeepAlive**: The script now configures SSH connections with `ServerAliveInterval` to prevent intermediate firewalls or routers from silently dropping connections during long, quiet transfers.
*   **Circuit Breaker**: For torrents that repeatedly fail to transfer, a circuit breaker pattern is employed. After a configurable number of failed attempts, the torrent is temporarily skipped, preventing it from blocking the queue of other, healthy torrents. The script will retry the failed torrent after a cool-down period.
*   **Error Handling**: "Provisioning Errors" (e.g., permission failures) are now distinguished from general transfer failures, allowing for more targeted troubleshooting.

## Requirements

*   Python 3.6+
*   Two qBittorrent clients accessible over the network.
*   SSH/SFTP access to the source server (and destination server if using `sftp_upload` mode).

## Installation & Setup

It's highly recommended to run this script in a Python virtual environment.

## Contributing

### Governance & Policies

*   [Security Policy](SECURITY.md)
*   [Release Confidence & QA](RELEASE_CONFIDENCE.md)
*   **[AI Agents & Contributors](AGENTS.md)**: Essential guidance for AI agents and human contributors.

Please read [ARCHITECTURE.md](ARCHITECTURE.md) for an overview of the system architecture and guidelines for making changes.

### 1. Get the Code and Prepare the Environment

First, clone the repository.

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
pip install -r requirements.txt
```

### 3. Install `sshpass` (Required for Rsync)

If you plan to use the `rsync` transfer mode, you must install `sshpass`.

*   **For Debian/Ubuntu:** `sudo apt-get update && sudo apt-get install sshpass`

**Note for `rsync` and `rsync_upload` modes**: Both modes require `rsync` and `sshpass` to be installed on the **local machine** (the one running this script).
*   **For Fedora/CentOS/RHEL:** `sudo yum install sshpass`
*   **For Arch Linux:** `sudo pacman -S sshpass`

### 4. Create and Edit Your Configuration

```bash
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
    *   `watchdog_timeout`: (Optional) Time in seconds to wait for any sign of life from a transfer before killing it. Defaults to `1500` (25 minutes). Increase this if you transfer very large files (e.g., 4K Remuxes) that may take a long time to checksum.
    *   `pool_wait_timeout`: (Optional) Time in seconds to wait for a connection from the SSH pool if it's full. Defaults to `300`. Increase this if you get `TimeoutError` logs.

## Testing

To run the full test suite, execute the following command:

```bash
python3 -m unittest discover tests
```

As `pytest` is included in `requirements-dev.txt`, you can also use it as an alternative test runner:

```bash
pytest
```

## Basic Usage

### Testing Your Setup (Recommended)

*   **Test Permissions**: Before the first run, verify that the script has the correct write permissions on the destination. It intelligently checks the local or remote path based on your `transfer_mode`.
    ```bash
    python3 torrent_mover.py --test-permissions
    ```

*   **Dry Run (Simulation)**: This is the safest mode. It prints all the actions it *would* take without transferring files or changing any torrents.
    ```bash
    python3 torrent_mover.py --dry-run
    ```

*   **Test Run (No Deletion)**: This performs a full run—transferring files and adding them to the destination—but **skips the final step of deleting the torrent from the source**.
    ```bash
    python3 torrent_mover.py --test-run
    ```

### Normal Run

Once you've confirmed everything works, run the script normally:

```bash
python3 torrent_mover.py
```

**Startup Sequence**: The application now performs "Pre-flight Checks" (Connectivity, Disk Space) with visible console output before launching the main dashboard.

## Scheduling with Cron

To run the script automatically, set up a cron job. **You must use absolute paths.**

1.  Open your crontab for editing: `crontab -e`
2.  Add a line to schedule the script. This example runs every 30 minutes.

```crontab
# Run the torrent mover every 30 minutes
*/30 * * * * cd /opt/torrent-mover && .venv/bin/python3 torrent_mover.py >> /opt/torrent-mover/cron.log 2>&1
```

**Breakdown of the command:**
*   `cd /opt/torrent-mover`: **Crucially**, first change to the repository's root directory.
*   `.venv/bin/python3`: Path to the Python executable in your virtual environment.
*   `torrent_mover.py`: Runs the script.
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
    python3 torrent_mover.py -c
    ```
*   **Manual Rule Management**:
    *   **List**: `python3 torrent_mover.py -l`
    *   **Add/Update**: `python3 torrent_mover.py -a "some-tracker.org" "My-TV-Shows"`
    *   **Delete**: `python3 torrent_mover.py -d "some-tracker.org"`

**Note on "Ignore" Rules**: Torrents matching a rule set to "Ignore" (case-insensitive) are **strictly excluded** from the processing queue. They will not be analyzed, moved, or touched by the script.
