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

### Important: First Run

It is **strongly recommended** to perform a dry run first to ensure your configuration is correct and the script identifies the correct torrents and paths.

```bash
python torrent_mover.py --dry-run
```

The `--dry-run` flag will print all the actions it *would* take without actually transferring files, adding torrents, or deleting anything. Review the output carefully.

### Normal Run

Once you have confirmed the dry run looks correct, you can run the script normally:

```bash
python torrent_mover.py
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
