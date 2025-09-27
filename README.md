# Torrent Mover

A Python script to automatically move completed torrents from a source qBittorrent client (like a seedbox) to a destination client (like a home server), transferring the data securely via SFTP or rsync.

## Versioning

This project follows a `MAJOR.MINOR.PATCH` versioning scheme:

*   **MAJOR**: Incremented for significant, backward-incompatible changes.
*   **MINOR**: Incremented when new, backward-compatible functionality is added.
*   **PATCH**: Incremented for backward-compatible bug fixes or minor updates.

The current version is **0.1.1**. To check your version, run: `python torrent_mover.py --version`.

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

It's highly recommended to run this script in a Python virtual environment to avoid conflicts with system packages.

### 1. Get the Code

First, clone this repository to a directory on your destination server (e.g., your Unraid server).

```bash
# Example: Clone into the /opt directory
cd /opt
git clone https://github.com/your-username/torrent-mover.git
cd torrent-mover
```

### 2. Set up Python Environment

Navigate into the project's `torrent_mover` subdirectory and create a virtual environment.

```bash
# Navigate to the script directory
cd torrent_mover

# Create a virtual environment
python3 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate
```
*Note: You will need to activate the virtual environment (`source .venv/bin/activate`) every time you open a new terminal session to run the script.*

### 3. Install Dependencies

Install the required Python libraries.

```bash
pip install -r requirements.txt
```

### 4. Create and Edit Your Configuration

Copy the template to create your own configuration file.

```bash
cp config.ini.template config.ini
```

Now, open `config.ini` with a text editor (like `nano`) and fill in your server details.

*   **[MANDARIN_QBIT]** - Your **source** qBittorrent client (e.g., your seedbox).
*   **[UNRAID_QBIT]** - Your **destination** qBittorrent client (e.g., your home server).
*   **[MANDARIN_SFTP]** - The SFTP/SSH details for your **source** server.
    *   `source_path`: The **absolute path** where the source qBittorrent saves completed files for the moving category. This must match the path set in qBittorrent.
*   **[UNRAID_PATHS]**
    *   `destination_path`: The **absolute local path** on the server where the script is running. This is where the torrent files will be moved to.
*   **[SETTINGS]**
    *   `category_to_move`: The category in qBittorrent that tells the script which torrents to move.

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

To run the script automatically, you can set up a cron job.

1.  Open your crontab for editing: `crontab -e`
2.  Add a line to schedule the script. This example runs it every hour and logs the output.

**Make sure to use absolute paths** for both the Python interpreter in your virtual environment and the script itself.

```crontab
# Run the torrent mover script every hour and log output
0 * * * * /path/to/your/torrent_mover/.venv/bin/python /path/to/your/torrent_mover/torrent_mover.py >> /path/to/your/torrent_mover/mover.log 2>&1
```
*   The `>> ... 2>&1` part is highly recommended, as it saves a log of the script's activity, which is essential for debugging.

## Command-Line Arguments

| Argument | Description |
|---|---|
| `--config [PATH]` | Specifies the path to the `config.ini` file. Defaults to `config.ini`. |
| `--dry-run` | Simulates the process without making any changes. |
| `--test-run` | Performs a full run but **skips deleting torrents** from the source client. |
| `--parallel-jobs [N]` | Sets the number of torrents to process concurrently. Defaults to `4`. |
| `--list-rules` | Lists all saved tracker-to-category rules and exits. |
| `--add-rule [DOMAIN] [CATEGORY]` | Adds or updates a categorization rule and exits. |
| `--delete-rule [DOMAIN]` | Deletes a specific categorization rule and exits. |
| `--interactive-categorize` | Starts an interactive session to create rules for uncategorized torrents. |
| `--version` | Displays the current version of the script and exits. |

## Advanced Usage

### Tracker-Based Categorization

This script can automatically assign a category to torrents on your destination client based on their trackers, helping you organize your library.

*   **How it Works**: The script can be "taught" which category to apply to a tracker domain. These rules are stored in `tracker_rules.json`.
*   **Interactive Learning**: The easiest way to create rules is to run the script in interactive mode. It will scan torrents on your destination client and, for any torrent whose tracker is unknown, prompt you to assign a category.
    ```bash
    python torrent_mover.py --interactive-categorize
    ```
*   **Manual Rule Management**: You can also manage rules directly from the command line:
    *   **List All Rules**: `python torrent_mover.py --list-rules`
    *   **Add/Update a Rule**: `python torrent_mover.py --add-rule "some-tracker.org" "My-TV-Shows"`
    *   **Delete a Rule**: `python torrent_mover.py --delete-rule "some-tracker.org"`

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