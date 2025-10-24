# Contributing to Torrent Mover

First off, thank you for considering contributing to Torrent Mover! It's people like you that make open source great.

Following these guidelines helps to communicate that you respect the time of the developers managing and developing this open source project. In return, they should reciprocate that respect in addressing your issue or assessing patches and features.

## Getting Started

### Prerequisites

*   Python 3.8+
*   `pip` for installing dependencies
*   `sshpass` is required if you plan to use the `rsync` transfer mode with password authentication.

### Setting Up the Environment

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/your-username/torrent-mover.git
    cd torrent-mover
    ```

2.  **Install the required dependencies:**
    ```bash
    pip install -r torrent_mover/requirements.txt
    ```

3.  **Configure the application:**
    *   Copy the `config.ini.template` to `config.ini`:
        ```bash
        cp torrent_mover/config.ini.template torrent_mover/config.ini
        ```
    *   Edit `torrent_mover/config.ini` with your server details, paths, and preferences. The template contains comments explaining each option.

### Running the Script

The script is designed to be run as a Python module from the root directory of the repository:

```bash
python -m torrent_mover.torrent_mover
```

You can use the `--help` flag to see all available command-line options:

```bash
python -m torrent_mover.torrent_mover --help
```

A good way to test your setup without making any actual changes is to use the `--dry-run` flag.

## How Can I Contribute?

### Reporting Bugs

If you find a bug, please open an issue on GitHub. Make sure to include:

*   A clear and descriptive title.
*   A detailed description of the problem, including steps to reproduce it.
*   The version of Torrent Mover you are using.
*   Your `config.ini` file (with any sensitive information like passwords redacted).
*   The relevant parts of the log file from the `logs/` directory.

### Suggesting Enhancements

If you have an idea for a new feature or an improvement to an existing one, please open an issue to discuss it. This allows us to coordinate our efforts and make sure your contribution aligns with the project's goals.

### Submitting Pull Requests

1.  **Fork the repository** and create your branch from `main`.
2.  **Make your changes.** Please adhere to the coding style and guidelines below.
3.  **Add or update tests** if your changes affect the core logic.
4.  **Ensure all tests pass.**
5.  **Update the documentation** if you've added or changed any features.
6.  **Submit your pull request** with a clear description of the changes you've made.

## Development Guidelines

### Coding Style

*   This project follows the **PEP 8** style guide for Python code.
*   Use type hints for function signatures.
*   Write clear and concise comments where necessary to explain complex logic.
*   Keep functions small and focused on a single task.

### Project Structure

*   `torrent_mover/torrent_mover.py`: The main application logic.
*   `torrent_mover/ui.py`: Handles the Rich-based terminal UI.
*   `torrent_mover/utils.py`: Contains helper functions and classes used across the application.
*   `torrent_mover/config.ini.template`: The template for the configuration file.
*   `torrent_mover/requirements.txt`: A list of Python dependencies.

### Commit Messages

Please follow the conventional commit message format. This makes the commit history easier to read and helps with automated versioning.

*   **feat:** A new feature.
*   **fix:** A bug fix.
*   **docs:** Changes to documentation.
*   **style:** Changes that do not affect the meaning of the code (white-space, formatting, etc.).
*   **refactor:** A code change that neither fixes a bug nor adds a feature.
*   **test:** Adding missing tests or correcting existing tests.
*   **chore:** Changes to the build process or auxiliary tools.

Example: `feat: Add support for Deluge torrent client`

### Versioning

This project follows Semantic Versioning (`MAJOR.MINOR.PATCH`). The official version is set in the `__version__` variable in `torrent_mover/torrent_mover.py`.

When you submit a pull request, please **do not** bump the version number yourself. The project maintainer will update the version in `torrent_mover/torrent_mover.py`, `README.md`, and any other relevant files upon merging, based on the conventional commit type of your contribution.
