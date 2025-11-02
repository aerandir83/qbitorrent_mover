"""
This module contains the FastAPI web server for the Torrent Mover application.

It provides a web-based UI for configuring the application, triggering transfers,
and viewing the status and logs of transfer operations.
"""
import uvicorn
import argparse
import configparser
from fastapi import FastAPI, HTTPException, Body
from fastapi.staticfiles import StaticFiles
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import logging
import sys
from typing import Dict, Any

# Project imports
from .config_manager import load_config_as_dict, save_config_from_dict, load_config
from .torrent_mover import __version__
from .system_manager import LockFile
from .tracker_manager import load_tracker_rules
from .transfer_manager import TransferCheckpoint, FileTransferTracker
from .ui import SimpleUIManager # We will use SimpleUIManager for non-visual logging


app = FastAPI(title="Torrent Mover API", version=__version__)

# Global state for the web UI
APP_STATE: Dict[str, Any] = {
    "status": "idle",
    "message": "Server started. Waiting for tasks.",
    "last_run_log": []
}

# Thread pool for running the transfer in the background
transfer_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="TransferWorker")

# Path to the config file (assuming it's next to this module)
SCRIPT_DIR = Path(__file__).resolve().parent
CONFIG_PATH = str(SCRIPT_DIR / 'config.ini')
CHECKPOINT = TransferCheckpoint(SCRIPT_DIR / 'transfer_checkpoint.json')
FILE_TRACKER = FileTransferTracker(SCRIPT_DIR / 'file_transfer_tracker.json')

# A custom logging handler to capture logs for the UI
class WebUILogHandler(logging.Handler):
    """A custom logging handler that captures log records for the web UI."""
    def emit(self, record):
        """
        Formats and appends a log record to the application state.

        Args:
            record: The log record to emit.
        """
        log_entry = self.format(record)
        APP_STATE["last_run_log"].append(log_entry)
        if len(APP_STATE["last_run_log"]) > 100:
            APP_STATE["last_run_log"].pop(0)

web_log_handler = WebUILogHandler()
web_log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Get the root logger for the 'torrent_mover' package
package_logger = logging.getLogger('torrent_mover')
package_logger.addHandler(web_log_handler)
package_logger.setLevel(logging.INFO)


def _run_transfer_in_background():
    """
    Wrapper function to run the transfer logic in a background thread
    and update the APP_STATE.
    """
    # Check for lock
    lock = None
    try:
        lock = LockFile(SCRIPT_DIR / 'torrent_mover.lock')
        lock.acquire()
    except RuntimeError as e:
        APP_STATE["status"] = "error"
        APP_STATE["message"] = str(e)
        return

    try:
        APP_STATE["status"] = "running"
        APP_STATE["message"] = "Transfer process started..."
        APP_STATE["last_run_log"] = ["--- Transfer Started ---"]

        # Load config and create dummy args for the transfer logic
        config = load_config(CONFIG_PATH)
        args = argparse.Namespace(
            dry_run=False,
            test_run=False,
            debug=True, # Log debug to web UI
            parallel_jobs=config['SETTINGS'].getint('parallel_jobs', 4),
            simple=True # Must use simple UI
        )

        # Use SimpleUIManager to log to our handler
        ui_manager = SimpleUIManager()

        # Run the core logic
        from .torrent_mover import run_transfer_logic
        run_transfer_logic(config, args, CHECKPOINT, FILE_TRACKER, ui_manager)

        APP_STATE["message"] = "Transfer process finished."
    except Exception as e:
        logging.error(f"Background transfer failed: {e}", exc_info=True)
        APP_STATE["status"] = "error"
        APP_STATE["message"] = f"Transfer failed: {e}"
    finally:
        APP_STATE["status"] = "idle"
        if lock:
            lock.release()


@app.get("/api/status")
async def get_status():
    """
    Retrieves the current status of the application.

    Returns:
        The application state dictionary.
    """
    return APP_STATE


@app.get("/api/config")
async def get_config():
    """
    Retrieves the current configuration as a dictionary.

    Raises:
        HTTPException: If the configuration cannot be loaded.

    Returns:
        The configuration dictionary.
    """
    try:
        return load_config_as_dict(CONFIG_PATH)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/config")
async def set_config(config_data: Dict[str, Dict[str, str]] = Body(...)):
    """
    Saves the provided configuration data to the config file.

    Args:
        config_data: A dictionary representing the configuration.

    Raises:
        HTTPException: If the configuration fails to save.

    Returns:
        A success message.
    """
    try:
        if save_config_from_dict(CONFIG_PATH, config_data):
            return {"message": "Configuration saved successfully."}
        else:
            raise HTTPException(status_code=500, detail="Failed to save configuration.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/trigger-transfer")
async def trigger_transfer():
    """
    Triggers a new torrent transfer process in the background.

    Raises:
        HTTPException: If a transfer is already in progress.

    Returns:
        A message indicating the transfer was triggered.
    """
    if APP_STATE["status"] == "running":
        raise HTTPException(status_code=409, detail="A transfer is already in progress.")

    transfer_executor.submit(_run_transfer_in_background)
    return {"message": "Transfer triggered successfully."}


# Mount the static directory for the frontend
# This assumes the HTML file will be in 'web/' relative to this file
static_dir = SCRIPT_DIR / "web"
app.mount("/", StaticFiles(directory=static_dir, html=True), name="web")


def start_server(config: configparser.ConfigParser):
    """
    Initializes and starts the uvicorn server for the web UI.
    """
    try:
        # config = load_config(CONFIG_PATH) # <-- REMOVED: Config is now passed in
        host = config.get('WEB_UI', 'host', fallback='127.0.0.1')
        port = config.getint('WEB_UI', 'port', fallback=8000)

        print(f"--- Starting Torrent Mover Web UI ---")
        print(f"Access the UI at: http://{host}:{port}")

        uvicorn.run(app, host=host, port=port)
    except Exception as e:
        print(f"FATAL: Could not start web server: {e}", file=sys.stderr)


if __name__ == "__main__":
    # This allows running the web server directly for testing
    config = load_config(CONFIG_PATH)
    start_server(config)
