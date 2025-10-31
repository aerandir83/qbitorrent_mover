# Torrent Mover

A Python script to automatically move completed torrents from a source qBittorrent client to a destination, with flexible and secure transfer options.

## Versioning

This project follows a `MAJOR.MINOR.PATCH` versioning scheme:

*   **MAJOR**: Incremented for significant, backward-incompatible changes.
*   **MINOR**: Incremented when new, backward-compatible functionality is added.
*   **PATCH**: Incremented for backward-compatible bug fixes or minor updates.

The current version is **2.7.3**. To check your version, run: `python3 -m torrent_mover.torrent_mover --version`.

## Changelog

### Version 2.7.3 (Latest)
* **fix(main)**: Fixed hang in `--simple` mode by refactoring all console logging logic. The script now correctly adds *only* a `StreamHandler` in simple mode and *only* a `RichHandler` in rich mode, resolving the `screen` conflict.

## Features

*   **Dual qBittorrent Client Support**: Manages torrents on both a source and a destination client via their WebUIs.
*   **Multiple Transfer Modes**:
    *   **`sftp`**: (Default) Securely downloads files from the source to the local machine running the script.
    *   **`sftp_upload`**: Securely transfers files directly from a source SFTP server to a destination SFTP server, ideal for server-to-server moves.
    *   **`rsync`**: Uses the `rsync` utility for potentially faster transfers to the local machine, with support for delta-transfers.
*   **Concurrent & Resumable Transfers**: Downloads or uploads multiple files in parallel to maximize speed. Failed transfers can be automatically resumed.
*   **Rich Interactive UI**: A terminal-based interface powered by `rich` provides real-time progress for overall transfers, individual torrents, and active files.
*   **Simple UI Mode**: A `--simple` flag provides a non-interactive, logging-based output suitable for `cron` jobs, `screen`/`tmux` sessions, or systemd services.
*   **Automated Workflow**:
    1.  Pauses the torrent on the source client.
    2.  Transfers the data to the destination.
    3.  Adds the torrent to the destination client (paused).
    4.  Triggers a force recheck to verify data integrity.
    5.  Starts the torrent on the destination for seeding.
    6.  Deletes the torrent and its data from the source to free up space.
*   **Safe Testing Modes**: Includes `--dry-run` and `--test-run` flags to simulate the process without making permanent changes.
*   **Health Checks**: Performs pre-run checks for destination disk space and write permissions.
*   **Flexible Configuration**: All settings are managed in a simple `config.ini` file, which is automatically updated with new options from a template.
*   **Advanced Categorization**: Optionally, automatically assign categories on your destination client based on torrent trackers.
*   **Robust Error Handling**: If a torrent fails the recheck after transfer, its data on the destination is deleted, and the torrent is marked internally to prevent automatic retries.

## Project Structure

The project is organized into a modular Python package for clarity and maintainability:

*   `torrent_mover.py`: The main entrypoint and orchestrator.
*   `config_manager.py`: Handles loading, updating, and validating `config.ini`.
*   `qbittorrent_manager.py`: Manages all interactions with qBittorrent clients.
*   `ssh_manager.py`: Manages SSH/SFTP connections, including a thread-safe connection pool.
*   `transfer_manager.py`: Contains the core logic for all file transfer modes.
*   `system_manager.py`: Handles system-level tasks like logging, process locking, and health checks.
*   `tracker_manager.py`: Manages tracker-based categorization rules.
*   `ui.py`: Defines the `rich` interactive UI and the simple logging UI.
*   `utils.py`: Contains shared utilities, such as the `@retry` decorator.

## Requirements

*   Python 3.6+
*   Two qBittorrent clients accessible over the network.
*   SSH/SFTP access to the source server (and destination server if using `sftp_upload` mode).
*   `sshpass` (required only for `rsync` mode).

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
| `--debug` | | Enable debug logging to file. |
| `--simple` | | Use a simple, non-interactive UI. Recommended for `screen` or `tmux` to avoid visual glitches. |
| `--parallel-jobs [N]` | | Sets the number of *torrents* to process concurrently. Defaults to `4`. |
| `--test-permissions` | | Tests write permissions for the configured destination and exits. |
| `--check-config` | | Validates the configuration file and exits. |
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

<details>
<summary>Full Changelog</summary>

### Version 2.7.2
* **fix(ui)**: Implemented all stubbed methods in `SimpleUIManager` to correctly log progress to `stdout`, fixing the issue where `--simple` mode produced no output.

### Version 2.7.1
* **fix(main)**: Corrected a `NameError` for `UIManager` by updating function definition type hints to use the `BaseUIManager` interface.

### Version 2.7.0
* **feat(ui)**: Added a `--simple` flag to use a basic, non-interactive logging UI.
* **feat(ui)**: The script now detects if it's running in `gnu screen` and will warn the user, recommending they use the `--simple` flag to avoid UI corruption.
* **refactor(ui)**: Created a `BaseUIManager` interface to support both the "rich" (`UIManagerV2`) and new `SimpleUIManager` modes.

</details>
