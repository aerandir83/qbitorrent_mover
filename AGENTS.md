# AGENTS.md

This document provides guidance for AI agents working on the Torrent Mover codebase.

## Project Overview

Torrent Mover is a Python application designed to automatically move completed torrents from a source client (like qBittorrent) to a destination client, transferring the associated data via SFTP or rsync.

### Core Components

*   **`torrent_mover/torrent_mover.py`**: The main application entrypoint. It initializes all manager classes and orchestrates the main application flow. `transfer_torrent` now uses the Strategy pattern to orchestrate transfers.
*   **`torrent_mover/ui.py`**: Defines the UI managers. It now uses a responsive layout.
    * `BaseUIManager`: An abstract interface for all UI implementations.
    * `UIManagerV2`: The "rich" UI, using `rich.Live` for an interactive, multi-panel display.
    * `SimpleUIManager`: The "simple" UI (for `--simple` mode), which only prints standard log lines.
*   **`torrent_mover/utils.py`**: Contains shared utilities, specifically the `@retry` decorator for handling transient network errors.
*   **`torrent_mover/config_manager.py`**: Handles loading, updating, and validating `config.ini`.
*   **`torrent_mover/ssh_manager.py`**: Manages all SSH/SFTP/Rsync connections and utilities (e.g., `SSHConnectionPool`, `sftp_mkdir_p`).
*   **`torrent_mover/qbittorrent_manager.py`**: Manages all direct interactions with the qBittorrent WebAPI (e.g., `connect_qbit`, `get_eligible_torrents`).
*   **`torrent_mover/transfer_manager.py`**: Contains the core transfer execution logic (e.g., `transfer_content_sftp_upload`) and the `FileTransferTracker`. Orchestration is now handled by strategies.
*   **`torrent_mover/transfer_strategies.py`**: Implements the Strategy pattern for different transfer modes.
*   **`torrent_mover/system_manager.py`**: Manages system-level tasks like logging, health checks, and cache cleanup. The `LockFile` is now PID-aware.
*   **`torrent_mover/tracker_manager.py`**: Manages all logic for tracker-based categorization (e.g., `load_tracker_rules`, `run_interactive_categorization`).
*   **`torrent_mover/watchdog.py`**: Monitors for hung transfers.
*   **`torrent_mover/resilient_queue.py`**: Manages the resilient transfer queue and circuit breaker.
*   **`torrent_mover/config.ini.template`**: The template for `config.ini`. When adding new configuration options, always update this file. The script will automatically update a user's `config.ini` from this template.

## Development Workflow

### 1. Understanding the Goal

Before making any changes, thoroughly read the user's request to understand the goal. The existing codebase and memories provide context on how the application is structured and intended to work.

### 2. Setting Up the Environment

To run the script and any tests, ensure the necessary dependencies are installed:

```bash
pip install -r torrent_mover/requirements.txt
```

A `config.ini` file is required for the script to run. You can create one by copying the template:

```bash
cp torrent_mover/config.ini.template torrent_mover/config.ini
```

The default `config.ini` may not have valid server details, but it's often sufficient for static analysis or running utility commands.

### 3. Running the Script

The main application is run as a module from the root of the repository:

```bash
python -m torrent_mover.torrent_mover
```

Key command-line flags for development and testing include:

*   `--dry-run`: This is the most important flag for testing. It executes the entire logic of the script (analysis, health checks) but **skips all file transfers and client actions** (adding/deleting torrents). Use this to verify your changes without performing real operations.
*   `--test-permissions`: Checks if the configured user has the necessary write permissions on the destination path.
*   `--debug`: Enables verbose debug logging to the log file.

### 4. Code Modifications

*   **Maintain Separation of Concerns**: Logic is separated into specialized manager modules. Ensure your changes respect this structure:
    *   UI changes go in `ui.py`.
    *   qBittorrent API interactions go in `qbittorrent_manager.py`.
    *   File transfer logic goes in `transfer_manager.py`.
    *   SSH/SFTP connection logic goes in `ssh_manager.py`.
    *   System-level tasks (e.g., logging, health checks) go in `system_manager.py`.
    *   Configuration logic goes in `config_manager.py`.
    *   Tracker-related logic goes in `tracker_manager.py`.
    *   The main orchestration logic is in `torrent_mover.py`.
*   **Configuration**: When adding a new setting, add it to `config.ini.template` with a descriptive comment. The `config_manager.py` module handles updating the user's config file.
*   **Error Handling**: Wrap network operations and file I/O in `try...except` blocks. Use the `@retry` decorator from `utils.py` for functions that might fail due to transient issues. For persistent recheck failures, the script now marks the torrent hash in `transfer_checkpoint.json` and skips it on future runs to prevent repeated failures. A CLI command (`--clear-recheck-failure`) is provided for manual recovery.
*   **UI Updates**: The `UIManagerV2` in `ui.py` is thread-safe. Update it from worker threads to show real-time progress. Do not modify the UI directly from other modules; call the appropriate `UIManagerV2` methods.
*   **Concurrency**: The script uses a `ThreadPoolExecutor` for concurrent file transfers and torrent analysis. Ensure that any functions called within the executor are thread-safe. The `SSHConnectionPool` is designed for this purpose.

### 5. Versioning (Mandatory)

This project follows Semantic Versioning (`MAJOR.MINOR.PATCH`). The canonical version number is defined as the `__version__` variable within `torrent_mover/torrent_mover.py`.

When you submit a pull request, please **do not** bump the version number yourself. The project maintainer will update the version in `torrent_mover/torrent_mover.py`, `README.md`, and any other relevant files upon merging, based on the conventional commit type of your contribution.

### 6. Pre-Commit Steps and Submission

Before submitting your changes, ensure you have:
1.  **Run any relevant tests**: While this project doesn't have a dedicated test suite yet, use `--dry-run` and `--test-permissions` to simulate your changes and catch potential issues.
2.  **Verified your changes**: Manually inspect the code to ensure it meets the request's requirements and adheres to the project's coding style.
3.  **Followed the instructions from `pre_commit_instructions`**: This is a mandatory step.

When submitting, use a clear and descriptive commit message that follows the conventional commit format (e.g., `feat: ...`, `fix: ...`).
