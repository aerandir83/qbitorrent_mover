# AGENTS.md

This document provides guidance for AI agents working on the Torrent Mover codebase.

## Project Overview

Torrent Mover is a Python application designed to automatically move completed torrents from a source client (like qBittorrent) to a destination client, transferring the associated data via SFTP or rsync.

### Core Components

*   **`torrent_mover/torrent_mover.py`**: This is the main script containing the primary application logic. It handles configuration, connecting to torrent clients and servers, analyzing torrents, and coordinating the transfer process.
*   **`torrent_mover/ui.py`**: This module manages the terminal user interface using the `rich` library. It's responsible for displaying progress bars, status updates, and tables of torrents being processed. All UI-related changes should be made here.
*   **`torrent_mover/utils.py`**: This contains shared utilities, most notably the `SSHConnectionPool` for managing and reusing SSH/SFTP connections, and a `@retry` decorator for handling transient network errors.
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

*   **Maintain Separation of Concerns**: Keep application logic in `torrent_mover.py`, UI logic in `ui.py`, and reusable utilities in `utils.py`.
*   **Configuration**: When adding a new setting, add it to `config.ini.template` with a descriptive comment. The main script handles updating the user's config file.
*   **Error Handling**: Wrap network operations and file I/O in `try...except` blocks. Use the `@retry` decorator from `utils.py` for functions that might fail due to transient issues.
*   **UI Updates**: The `UIManager` in `ui.py` is thread-safe. Update it from worker threads to show real-time progress. Do not modify the UI directly from `torrent_mover.py`; call the appropriate `UIManager` methods.
*   **Concurrency**: The script uses a `ThreadPoolExecutor` for concurrent file transfers and torrent analysis. Ensure that any functions called within the executor are thread-safe. The `SSHConnectionPool` is designed for this purpose.

### 5. Versioning (Mandatory)

This project follows Semantic Versioning (`MAJOR.MINOR.PATCH`). The canonical version number is defined as the `__version__` variable within `torrent_mover/torrent_mover.py`.

**You, the AI agent, are responsible for updating this version number on every code change that warrants it.**

Use the following rules based on your conventional commit type:

*   **`fix:`**: Increment the **PATCH** version (e.g., `1.6.0` -> `1.6.1`). This is for backward-compatible bug fixes.
*   **`feat:`**: Increment the **MINOR** version and reset PATCH to zero (e.g., `1.6.0` -> `1.7.0`). This is for new, backward-compatible features.
*   **Breaking Change (`feat!:`, `fix!:`):** Increment the **MAJOR** version and reset MINOR and PATCH to zero (e.g., `1.6.0` -> `2.0.0`). This is for any change that is not backward-compatible.
*   **`refactor:`, `style:`, `docs:`, `chore:`, `test:`**: These types **do not** require a version bump.

**Mandatory Workflow:**
1. The user requests a code change (e.g., a new feature or a bug fix).
2. You implement the code to fulfill the request.
3. You determine the correct conventional commit type (e.g., `feat:`, `fix:`).
4. If the type is `feat:` or `fix:`, you **must** open `torrent_mover/torrent_mover.py`.
5. You **must** find the `__version__` variable and update it according to the rules above.
6. **You must also update the version number in `README.md` and any other files where the version is displayed.**
7. You will then proceed to the pre-commit and submission steps.

### 6. Pre-Commit Steps and Submission

Before submitting your changes, ensure you have:
1.  **Run any relevant tests**: While this project doesn't have a dedicated test suite yet, use `--dry-run` and `--test-permissions` to simulate your changes and catch potential issues.
2.  **Verified your changes**: Manually inspect the code to ensure it meets the request's requirements and adheres to the project's coding style.
3.  **Followed the instructions from `pre_commit_instructions`**: This is a mandatory step.

When submitting, use a clear and descriptive commit message that follows the conventional commit format (e.g., `feat: ...`, `fix: ...`).
