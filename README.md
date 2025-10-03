# Torrent Mover

A Python script to automatically move completed torrents from a source qBittorrent client (like a seedbox) to a destination client (like a home server), transferring the data securely via SFTP or rsync.

## Versioning

This project follows a `MAJOR.MINOR.PATCH` versioning scheme:

*   **MAJOR**: Incremented for significant, backward-incompatible changes.
*   **MINOR**: Incremented when new, backward-compatible functionality is added.
*   **PATCH**: Incremented for backward-compatible bug fixes or minor updates.

The current version is **1.2.1**. To check your version, run: `python torrent_mover.py --version`.

## Features

*   **Works with Two qBittorrent Clients**: Manages torrents on both a source and a destination client via their WebUIs.
*   **Secure & Fast File Transfers**: Uses `rsync` (recommended) or `sftp` to securely transfer torrent data.
*   **Category-Based Moving**: Only moves torrents assigned to a specific category you define (e.g., "move").
*   **Fully Automated Process**:
    1.  Pauses the torrent on the source client.
    2.  Transfers the data to the destination server.
    3.  Adds the torrent to the destination client (paused).
    4.  Triggers a force recheck to verify data integrity.
    5.  Starts the torrent on the destination for seeding.
    6.  Deletes the torrent and its data from the source to free up space.
*   **Safe Testing Modes**: Includes `--dry-run` and `--test-run` flags to simulate the process without making permanent changes, so you can safely test your configuration.
*   **Flexible Configuration**: All settings are managed in a simple `config.ini` file.
*   **Advanced Categorization**: Optionally, automatically assign categories on your destination client based on torrent trackers.

## Requirements

*   Python 3.6+
*   Two qBittorrent clients accessible over the network from where the script is run.
*   SSH/SFTP access to the source server.

## Installation & Setup

It's highly recommended to run this script in a Python virtual environment to keep its dependencies separate from system packages.

### 1. Get the Code and Prepare the Environment

First, clone the repository and navigate into the main script directory.

```bash
# Example: Clone into the /opt directory
cd /opt
git clone https://github.com/your-username/torrent-mover.git

# The script is located in the 'torrent_mover' subdirectory, so go there
cd torrent-mover/torrent_mover
```

Now, create and activate a Python virtual environment.

```bash
# Create the virtual environment inside the script directory
python3 -m venv .venv

# Activate it (you'll need to do this every time you open a new shell)
source .venv/bin/activate
```

### 2. Install Dependencies

With your virtual environment active, install the required Python libraries from `requirements.txt`.

```bash
pip install -r requirements.txt
```

### 3. Create and Edit Your Configuration

Copy the `config.ini.template` to create your own `config.ini` file.

```bash
cp config.ini.template config.ini
```

Now, open `config.ini` with a text editor (like `nano` or `vi`) and fill in your server details. The template is organized into the following sections:

*   **`[SOURCE_CLIENT]`**: Your **source** qBittorrent client (e.g., your seedbox).
*   **`[DESTINATION_CLIENT]`**: Your **destination** qBittorrent client (e.g., your home server).
*   **`[SOURCE_SERVER]`**: The SFTP/SSH details for your **source** server, where the torrent files are stored.
*   **`[DESTINATION_PATHS]`**: The path on the destination server where torrent data will be moved.
*   **`[SETTINGS]`**: Key operational settings. This is where you define:
    *   `source_client_section` and `destination_client_section`: The names of the sections that define your clients. By default, they are `SOURCE_CLIENT` and `DESTINATION_CLIENT`. If you rename these sections, you **must** update the names here to match.
    *   `category_to_move`: The category in your source client that triggers a move.

## Basic Usage

It is **strongly recommended** to test your configuration before a normal run.

### Testing Your Setup (Recommended)

*   **Dry Run (Simulation)**: This is the safest mode. It prints all the actions it *would* take without transferring files or changing any torrents. It's perfect for verifying your paths and connections.
    ```bash
    python torrent_mover.py --dry-run
    ```

*   **Test Run (No Deletion)**: This performs a full run—transferring files and adding them to the destination—but **skips the final step of deleting the torrent from the source**. Use this for end-to-end testing without risking data loss.
    ```bash
    python torrent_mover.py --test-run
    ```

### Normal Run

Once you've confirmed everything works as expected, run the script normally:

```bash
python torrent_mover.py
```

## Scheduling with Cron

To run the script automatically, you can set up a cron job. This allows the script to run on a schedule (e.g., every hour) without manual intervention.

1.  Open your crontab for editing: `crontab -e`
2.  Add a line to schedule the script. **You must use absolute paths** for the Python interpreter (inside your `.venv`) and the script itself.

Here is a robust example that runs the script every 30 minutes and saves its output to a log file.

```crontab
# Run the torrent mover every 30 minutes
# The full path to the python from your virtual environment is required.
# The full path to the script is required.
# Logging output to a file is highly recommended for debugging.
*/30 * * * * /opt/torrent-mover/torrent_mover/.venv/bin/python /opt/torrent-mover/torrent_mover/torrent_mover.py >> /opt/torrent-mover/torrent_mover/cron.log 2>&1
```

**Breakdown of the command:**
*   `*/30 * * * *`: The schedule, meaning "at minute 30 past every hour."
*   `/opt/torrent-mover/.../python`: Absolute path to the Python executable in your virtual environment.
*   `/opt/torrent-mover/.../torrent_mover.py`: Absolute path to the script.
*   `>> cron.log 2>&1`: Appends all output (both standard and error) to `cron.log`, which is useful for checking if the script ran correctly.

## Command-Line Arguments

| Argument | Alias | Description |
|---|---|---|
| `--config [PATH]` | | Specifies the path to the `config.ini` file. Defaults to `config.ini`. |
| `--dry-run` | | Simulates the process without making any changes. |
| `--test-run` | | Performs a full run but **skips deleting torrents** from the source client. |
| `--parallel-jobs [N]` | | Sets the number of torrents to process concurrently. Defaults to `4`. |
| `--categorize` | `-c` | Starts an interactive session to categorize torrents. Scans the `category_to_move` from your config by default. |
| `--category [CATEGORY]` | | Overrides the default category for a `--categorize` session. |
| `--no-rules` | `-nr` | During a `--categorize` session, ignore existing rules and review all torrents in the target category. |
| `--list-rules` | `-l` | Lists all saved tracker-to-category rules and exits. |
| `--add-rule [DOMAIN] [CATEGORY]` | `-a` | Adds or updates a categorization rule and exits. |
| `--delete-rule [DOMAIN]` | `-d` | Deletes a specific categorization rule and exits. |
| `--version` | | Displays the current version of the script and exits. |

## Advanced Usage

### Tracker-Based Categorization

This script can automatically assign a category to torrents on your destination client based on their trackers, helping you organize your library.

*   **How it Works**: The script can be "taught" which category to apply to a tracker domain. These rules are stored in `tracker_rules.json`.
*   **Interactive Learning**: The easiest way to create rules is by running the script in interactive categorization mode. By default, it scans for torrents in the category defined as `category_to_move` in your `config.ini`. For any torrent that doesn't match an existing rule, it will prompt you to assign a category and optionally save that choice as a new rule.
    ```bash
    # Start interactive mode, scanning the default category from your config
    python torrent_mover.py -c
    ```
    You can also specify a different category to scan, or force the script to ignore existing rules for the session:
    ```bash
    # Scan a specific category named "My-Downloads"
    python torrent_mover.py -c --category "My-Downloads"

    # Scan the default category but ignore all existing rules
    python torrent_mover.py -c -nr
    ```
*   **Manual Rule Management**: You can also manage rules directly from the command line using these shortcuts:
    *   **List All Rules**: `python torrent_mover.py -l`
    *   **Add/Update a Rule**: `python torrent_mover.py -a "some-tracker.org" "My-TV-Shows"`
    *   **Delete a Rule**: `python torrent_mover.py -d "some-tracker.org"`

### Enabling Bash Auto-Completion (Optional)

To enable `Tab` auto-completion for command-line arguments, follow these steps:

1.  **Make the script executable:**
    ```bash
    chmod +x torrent_mover.py
    ```

2.  **Find the absolute path to the script:**
    While in the `torrent_mover` directory, run `pwd`. The output is the absolute path to the directory.

3.  **Add the registration command to your `.bashrc`:**
    Add the following line to the end of your `~/.bashrc` file, replacing the placeholder path with the one you found above.
    ```bash
    # Add this to ~/.bashrc, replacing the path with your own
    eval "$(register-python-argcomplete /path/to/your/torrent_mover/torrent_mover.py)"
    ```

4.  **Reload your shell:**
    Run `source ~/.bashrc` or restart your shell. Auto-completion should now be active.