# Torrent Mover

A Python script to automatically move completed torrents and their data from a source qBittorrent client (e.g., a seedbox) to a destination qBittorrent client (e.g., a home server) and transfer the files via SFTP.

## Features

*   **Connects to Two qBittorrent Clients**: Manages torrents on both a source and a destination client via their WebUIs.
*   **Secure File Transfer**: Uses SFTP to securely transfer torrent data from the source server to the local machine where the script is run.
*   **Category-Based Moving**: Only moves torrents that are assigned a specific, user-defined category.
*   **Preserves Directory Structure**: Intelligently handles both single-file and multi-file torrents, perfectly recreating the directory structure on the destination.
*   **Automated Lifecycle Management**: Automates the entire workflow:
    1.  Pauses the torrent on the source client.
    2.  Transfers the data.
    3.  Adds the torrent to the destination client (paused).
    4.  Triggers a force recheck to verify data integrity.
    5.  Starts the torrent on the destination client for seeding.
    6.  Deletes the torrent and its data from the source client to free up space.
*   **Safe Dry Run Mode**: Includes a `--dry-run` flag to simulate the entire process without making any actual changes, allowing you to safely test your configuration.
*   **Flexible Configuration**: All server details, credentials, and paths are stored in a simple `config.ini` file.

## Advanced Features

### Tracker-Based Categorization

This script can automatically assign a category to torrents on the destination client based on their trackers. This helps in organizing your library without manual intervention.

*   **Learning System**: The script "learns" which category to apply to a tracker domain. When it processes a torrent with a tracker it hasn't seen before, you can run the script in interactive mode to teach it a new rule.
*   **Rule-Based**: The rules are stored in a simple `tracker_rules.json` file in the `torrent_mover` directory, which can be edited manually or via command-line options.
*   **Automatic Application**: During a normal run, after a torrent is transferred and re-checked on the destination client, the script will look up its tracker in the rules and apply the corresponding category.

## Requirements

*   Python 3.6+
*   Network access from where the script is run to both qBittorrent WebUIs.
*   SSH/SFTP access from where the script is run to the source server.
*   A dedicated directory on the source server for torrents that are ready to be moved.

## Setup Instructions

It is highly recommended to run this script within a Python virtual environment to avoid conflicts with system-wide packages. It is designed to run on your Unraid server (or an LXC container within it).

### 1. Set up Python Environment

If you are in an LXC container or a machine with Python, navigate to the `torrent_mover` directory and create a virtual environment:

```bash
# Navigate to the project directory
cd /path/to/torrent_mover

# Create a virtual environment
python3 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate
```

### 2. Install Dependencies

Install the required Python libraries using the `requirements.txt` file:

```bash
pip install -r requirements.txt
```

### 3. Create Your Configuration

Copy the template file to create your own configuration:

```bash
cp config.ini.template config.ini
```

Now, open `config.ini` with a text editor and fill in the details for your servers.

**[MANDARIN_QBIT]** - Your source qBittorrent client
*   `host`: The full URL to the client, e.g., `https://mandarin.example.com`
*   `port`: The port for the WebUI (e.g., `443` for HTTPS).
*   `username`/`password`: Your qBittorrent login credentials.
*   `verify_cert`: Set to `False` if you use a self-signed SSL certificate.

**[UNRAID_QBIT]** - Your destination qBittorrent client
*   (Same fields as above, but for your Unraid/home client)

**[MANDARIN_SFTP]** - SFTP details for your source server
*   `host`: The IP address or hostname for SFTP connection.
*   `port`: The SFTP port (usually `22`).
*   `username`/`password`: Your SFTP/SSH login credentials.
*   `source_path`: The **absolute path** on the source server where qBittorrent saves completed files for the moving category. **This must match the path in qBittorrent.**

**[UNRAID_PATHS]**
*   `destination_path`: The **absolute local path** on the server running the script where files should be moved to. This script assumes it is running on the destination machine (e.g., Unraid).

**[SETTINGS]**
*   `category_to_move`: The name of the category in qBittorrent that identifies torrents to be moved (e.g., `moving`).

## Usage

### Execution Modes

It is **strongly recommended** to test your configuration before running the script normally. There are two modes for testing:

*   **Dry Run (Simulation)**: This is the safest mode. It will print all the actions it *would* take (like which torrents it would move and where) without actually transferring files, adding torrents, or deleting anything. Use this to verify your paths and connections.
    ```bash
    python torrent_mover.py --dry-run
    ```

*   **Test Run (No Deletion)**: This mode performs a full run of the script—including pausing, transferring files, adding to the destination, and re-checking—but it **skips the final step of deleting the torrent from the source client**. This is useful for end-to-end testing of your setup without risking data loss on the source.
    ```bash
    python torrent_mover.py --test-run
    ```

### Normal Run

Once you have confirmed the dry run looks correct, you can run the script normally:

```bash
python torrent_mover.py
```

### Command-Line Arguments

The script supports several command-line arguments to customize its behavior:

| Argument | Description |
|---|---|
| `--config [PATH]` | Specifies the path to the `config.ini` file. Defaults to `config.ini` in the script directory. |
| `--dry-run` | Simulates the entire process, printing actions without making any changes. |
| `--test-run` | Performs a full run but **skips deleting torrents** from the source client. |
| `--parallel-jobs [N]` | Sets the number of torrents to process concurrently. Defaults to `4`. |
| `--list-rules` | Lists all saved tracker-to-category rules and exits. |
| `--add-rule [DOMAIN] [CATEGORY]` | Adds or updates a categorization rule and exits. |
| `--delete-rule [DOMAIN]` | Deletes a specific categorization rule and exits. |
| `--interactive-categorize` | Starts an interactive session to create rules for uncategorized torrents. |

### Enabling Bash Auto-Completion (Optional)

To make using the command-line arguments easier, you can enable bash auto-completion. This will allow you to press the `Tab` key to auto-complete arguments and their values.

1.  **Ensure you have installed the dependencies from `requirements.txt`**, as this includes the `argcomplete` library.

2.  **Run the global registration command:**
    ```bash
    eval "$(register-python-argcomplete torrent_mover.py)"
    ```

3.  **To make the change permanent**, add the command to your shell's startup file (e.g., `~/.bashrc` or `~/.zshrc`):
    ```bash
    echo 'eval "$(register-python-argcomplete torrent_mover.py)"' >> ~/.bashrc
    ```
    You will need to restart your shell or run `source ~/.bashrc` for the changes to take effect.

### Managing Tracker Categorization Rules

You can manage the tracker-to-category rules directly from the command line. These commands do not require a full run of the mover script.

*   **List All Rules**:
    ```bash
    python torrent_mover.py --list-rules
    ```

*   **Add or Update a Rule**:
    This command maps a tracker domain (e.g., `some-tracker.org`) to a category name that exists in your destination qBittorrent client.
    ```bash
    python torrent_mover.py --add-rule "some-tracker.org" "My-TV-Shows"
    ```

*   **Delete a Rule**:
    ```bash
    python torrent_mover.py --delete-rule "some-tracker.org"
    ```

*   **Interactively Create Rules (Learning Mode)**:
    The script will scan all torrents on your destination client. For any torrent whose tracker domain does not have a rule, it will prompt you to choose from a list of your existing qBittorrent categories. It's recommended to run this periodically to teach the script about new trackers.
    ```bash
    python torrent_mover.py --interactive-categorize
    ```

### Specifying a Config File

If you named your config file something else or placed it in a different directory, use the `--config` argument:

```bash
python torrent_mover.py --config /path/to/my/configuration.ini
```

## Scheduling with Cron

To run the script automatically, you can set up a cron job.

1.  Open your crontab for editing: `crontab -e`
2.  Add a new line to schedule the script. The following example runs the script every hour.

**Make sure to use absolute paths** for the Python interpreter and the script itself.

```crontab
# Run the torrent mover script every hour and log output to a file
0 * * * * /path/to/torrent_mover/.venv/bin/python /path/to/torrent_mover/torrent_mover.py >> /path/to/torrent_mover/mover.log 2>&1
```

*   `0 * * * *`: This means "at minute 0 of every hour".
*   `/path/to/torrent_mover/.venv/bin/python`: Absolute path to the Python interpreter inside your virtual environment.
*   `/path/to/torrent_mover/torrent_mover.py`: Absolute path to the script.
*   `>> /path/to/torrent_mover/mover.log 2>&1`: This appends all output (both standard output and errors) to a log file named `mover.log` inside the project directory. This is highly recommended for debugging.
```
