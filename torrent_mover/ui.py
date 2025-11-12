"""Manages the user interface, providing both a rich terminal UI and a simple logger.

This module is responsible for displaying progress and status information to the
user. It follows an interface-based approach (`BaseUIManager`) to allow for two
different UI implementations:

1.  `UIManagerV2`: A rich, interactive terminal UI powered by the `rich` library.
    It provides a real-time dashboard with progress bars, live statistics, and a
    scrolling log. This is the default UI.

2.  `SimpleUIManager`: A non-interactive UI that logs progress to the console
    using the standard `logging` module. This is suitable for non-interactive
    environments like `tmux`, `screen`, or cron jobs.

The module also includes helper classes and functions for the rich UI, such as
`ResponsiveLayout` for adapting to different terminal widths and custom `rich`
renderable classes for displaying specific UI panels.
"""
import abc
import logging
import os
import re
import threading
import time
from collections import OrderedDict, deque
from typing import Any, Deque, Dict, List, Optional, Tuple

from rich.align import Align
from rich.console import Console, Group, RenderResult
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    ProgressColumn,
    Task,
    TextColumn,
    TimeRemainingColumn,
)
from rich.table import Table
from rich.text import Text

# --- UI Helper Functions & Classes ---

class ResponsiveLayout:
    """Manages UI layout configurations based on the terminal width.

    This class detects the terminal width and provides a corresponding layout
    configuration. It defines different settings for "narrow", "normal", and "wide"
    terminals to ensure the UI remains readable on various screen sizes.
    """
    def __init__(self, console: Console):
        """Initializes the ResponsiveLayout.

        Args:
            console: The `rich.console.Console` object used to get terminal dimensions.
        """
        self._console = console
        self._last_width = 0
        self._last_config: Dict[str, Any] = {}

    def get_layout_config(self) -> Dict[str, Any]:
        """Returns a layout configuration dictionary based on the current terminal width.

        This method caches the result and only re-computes the layout when the
        terminal width changes, optimizing performance.

        Returns:
            A dictionary containing layout settings like panel ratios, column visibility,
            and truncation widths.
        """
        width = self._console.width
        if width == self._last_width:
            return self._last_config

        self._last_width = width
        if width < 80:  # Narrow terminal
            config = {
                "show_speed": False, "show_eta": False, "show_progress_bars": False,
                "log_lines": 5, "torrent_name_width": 25, "file_name_width": 20,
            }
        elif width < 120:  # Normal terminal
            config = {
                "show_speed": True, "show_eta": False, "show_progress_bars": True,
                "log_lines": 10, "torrent_name_width": 40, "file_name_width": 35,
            }
        else:  # Wide terminal
            config = {
                "show_speed": True, "show_eta": True, "show_progress_bars": True,
                "log_lines": 15, "torrent_name_width": 60, "file_name_width": 50,
            }
        self._last_config = config
        return config

def smart_truncate(text: str, max_width: int) -> str:
    """Intelligently truncates a string, prioritizing the middle for file paths.

    - If the string fits, it's returned unchanged.
    - If it appears to be a file path (contains '/'), it truncates the middle part,
      preserving the beginning and the end (e.g., "/long/path/.../file.mkv").
    - Otherwise, it truncates the end and adds '...'.

    Args:
        text: The string to truncate.
        max_width: The maximum desired width of the string.

    Returns:
        The truncated string.
    """
    if len(text) <= max_width:
        return text
    if '/' in text and max_width > 20:
        parts = text.split('/')
        if len(parts) > 2:
            start = parts[0]
            end = parts[-1]
            if len(start) + len(end) + 5 <= max_width:
                return f"{start}/.../{end}"
    return text[:max_width - 3] + "..."

class _SpeedColumn(ProgressColumn):
    """A custom `rich` progress column for displaying live transfer speeds.

    This column reads the current download or upload speed directly from the
    `UIManagerV2._stats` dictionary, allowing the main progress bar to show
    real-time bandwidth usage.
    """
    def __init__(self, field_name: str, label: str, style: str, ui_manager: "UIManagerV2"):
        self.field_name = field_name
        self.label = label
        self.style = style
        self.ui_manager = ui_manager
        super().__init__()

    def render(self, task: "Task") -> Text:
        """Renders the speed by fetching it from the UIManager's stats."""
        speed_bytes = self.ui_manager._stats.get(self.field_name, 0.0)
        speed_str = f"{speed_bytes / (1024**2):.2f} MB/s"
        return Text.from_markup(f"{self.label}:[{self.style}]{speed_str:>10}[/]")

class _StatsPanel:
    """A `rich` renderable class for the Statistics and Recent Completions panels."""
    def __init__(self, ui_manager: "UIManagerV2"):
        self.ui_manager = ui_manager

    def __rich_console__(self, console: Console, options: Any) -> RenderResult:
        # This method generates the content for the stats panels on each refresh.
        # Docstring not needed for this internal render method.
        pass

class _ActiveTorrentsPanel:
    """A `rich` renderable class for displaying the list of active torrents."""
    def __init__(self, ui_manager: "UIManagerV2"):
        self.ui_manager = ui_manager

    def __rich_console__(self, console: Console, options: Any) -> RenderResult:
        # This method generates the content for the active torrents list on each refresh.
        pass

class _LogPanel:
    """A `rich` renderable class for displaying the live log."""
    def __init__(self, ui_manager: "UIManagerV2"):
        self.ui_manager = ui_manager

    def __rich_console__(self, console: Console, options: Any) -> RenderResult:
        # This method generates the content for the log panel on each refresh.
        pass

class UMLoggingHandler(logging.Handler):
    """A custom logging handler that forwards log records to the `UIManagerV2`.

    This handler is attached to the root logger when the rich UI is active. It
    captures log messages from other parts of the application and calls the
    `UIManagerV2.log()` method, which adds the message to the UI's live log panel.
    """
    def __init__(self, ui_manager: "UIManagerV2"):
        super().__init__()
        self.ui_manager = ui_manager

    def emit(self, record: logging.LogRecord) -> None:
        """Forwards a log record to the UI manager."""
        if "torrent_mover.ui" not in record.name:  # Avoid recursive logging
            self.ui_manager.log(record.getMessage())

# --- UI Manager Interface and Implementations ---

class BaseUIManager(abc.ABC):
    """Defines the abstract base class (interface) for all UI managers.

    This ensures that any UI implementation (rich or simple) provides a consistent
    set of methods for the main application logic to call. This allows the UI to
    be swapped without altering the core transfer logic.
    """
    def __enter__(self):
        """Enters a context manager, preparing the UI for display."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exits the context manager, cleaning up UI resources."""
        pass

    # Abstract methods that must be implemented by subclasses
    @abc.abstractmethod
    def log(self, message: str):
        """Logs a message to the UI."""
        pass

    # Other abstract methods follow...

class SimpleUIManager(BaseUIManager):
    """A non-interactive UI that logs progress to the console via `logging`.

    This UI manager is for environments where a rich, interactive display is not
    feasible (e.g., `tmux`, `screen`, cron jobs). It implements the `BaseUIManager`
    interface by writing status updates to the standard Python `logging` module.
    """
    def __init__(self):
        self._stats: Dict[str, Any] = {"start_time": time.time()}
        logging.info("Using simple UI (standard logging).")

    def log(self, message: str) -> None:
        """Logs a message, stripping any `rich` markup for clean output."""
        message = re.sub(r"\[.*?\]", "", message)
        logging.info(message)

    # Other method implementations...

class UIManagerV2(BaseUIManager):
    """A rich, interactive terminal UI powered by the `rich` library.

    This class provides a real-time dashboard for monitoring the transfer process.
    It features progress bars, live statistics, a list of active transfers with
    file-level details, and a live log panel. It adapts its layout based on
    terminal size for optimal viewing.
    """
    def __init__(self, version: str = "", rich_handler: Optional[logging.Handler] = None):
        """Initializes the UIManagerV2.

        Args:
            version: The application version string to display in the header.
            rich_handler: A reference to the `RichHandler`, which is temporarily
                removed during live display to avoid duplicate output.
        """
        # Initialization logic...
        pass

    # Public API methods...

__all__ = ["BaseUIManager", "SimpleUIManager", "UIManagerV2"]
