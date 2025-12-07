import os
import time
from rich.console import Console, Group, RenderResult
from rich.live import Live
from rich.panel import Panel
from typing import Dict, Any
from watchdog import TransferWatchdog


class ResponsiveLayout:
    """Manages UI layout configurations based on terminal width."""
    def __init__(self, console: Console):
        """Initializes the ResponsiveLayout.

        Args:
            console: The rich Console object to get terminal dimensions from.
        """
        self._console = console
        self._last_width = 0
        self._last_config: Dict[str, Any] = {}

    def get_layout_config(self) -> Dict[str, Any]:
        """Returns a layout configuration dictionary based on current terminal width.

        Caches the result and only re-computes when the width changes.

        Returns:
            A dictionary with layout settings.
        """
        width = self._console.width
        if width == self._last_width:
            return self._last_config

        self._last_width = width
        if width < 80:  # Narrow
            config = {
                "terminal_width": "narrow",
                "left_ratio": 1,
                "right_ratio": 0,  # Hide stats panel
                "show_speed": False,
                "show_eta": False,
                "show_progress_bars": False,
                "recent_completions": 0,
                "terminal_height": 24,
                "log_lines": 5,
                "torrent_name_width": 25,
                "file_name_width": 20,
            }
        elif width < 120:  # Normal
            config = {
                "terminal_width": "normal",
                "left_ratio": 2,
                "right_ratio": 1,
                "show_speed": True,
                "show_eta": False,
                "show_progress_bars": True,
                "recent_completions": 3,
                "terminal_height": 30,
                "log_lines": 10,
                "torrent_name_width": 40,
                "file_name_width": 35,
            }
        else:  # Wide
            config = {
                "terminal_width": "wide",
                "left_ratio": 3,
                "right_ratio": 1,
                "show_speed": True,
                "show_eta": True,
                "show_progress_bars": True,
                "recent_completions": 5,
                "terminal_height": 40,
                "log_lines": 15,
                "torrent_name_width": 60,
                "file_name_width": 50,
            }
        self._last_config = config
        return config

def smart_truncate(text: str, max_width: int, min_width: int = 20) -> str:
    """Intelligently truncates a string, especially for file paths.

    - If the string is short enough, it's returned as is.
    - If it's a path, it attempts to truncate the middle part.
    - If it's a regular string, it truncates the end and adds '...'.

    Args:
        text: The string to truncate.
        max_width: The maximum desired width.
        min_width: The minimum width for middle truncation to be effective.

    Returns:
        The truncated string.
    """
    if len(text) <= max_width:
        return text

    # Handle file paths specially
    if '/' in text or '\\' in text:
        # Use os.path to handle both separators
        import os
        parts = text.split(os.sep)
        if len(parts) > 2 and max_width > min_width:
            start = parts[0]
            end = parts[-1]
            # Check if just the start and end are too long
            if len(start) + len(end) + 5 > max_width: # 5 for "/.../"
                 return text[:max_width - 3] + "..."
            middle = "/.../"
            return f"{start}{middle}{end}"

    # Standard truncation
    return text[:max_width - 3] + "..."
from rich.progress import (
Progress,
TextColumn,
BarColumn,
TransferSpeedColumn,
TimeRemainingColumn,
DownloadColumn,
TaskID,
ProgressColumn, # <-- Import ProgressColumn
)
from rich.table import Table
from rich.text import Text
from rich.align import Align
import logging
import threading
from collections import OrderedDict, deque
from typing import Dict, Any, Optional, Deque, Tuple, List, Type
from rich.layout import Layout
import re
import abc


logger = logging.getLogger("torrent_mover")

# --- Custom Progress Column for Speed ---

class _SpeedColumn(ProgressColumn):
    """Renders DL/UL speed by reading directly from ui_manager._stats."""
    def __init__(self, field_name: str, label: str, style: str, ui_manager: "UIManagerV2"):
        self.field_name = field_name # This will now be 'current_dl_speed' or 'current_ul_speed'
        self.label = label
        self.style = style
        self.ui_manager = ui_manager # <-- Store the UI Manager instance
        super().__init__()

    def render(self, task: "Task") -> Text:
        """Get speed from ui_manager._stats and render it."""
        speed_bytes = 0.0
        try:
            # Safely read the speed from the stats dictionary
            with self.ui_manager._lock:
                speed_bytes = self.ui_manager._stats.get(self.field_name, 0.0)
        except Exception:
            pass # In case lock fails, just render 0.0

        # Format the speed here, just like the Stats panel does
        speed_str = f"{speed_bytes / (1024**2):.2f} MB/s"

        # Conditionally show '--' for UL speed on non-uploading modes
        if speed_bytes == 0 and self.field_name == 'current_ul_speed':
            mode = self.ui_manager.transfer_mode
            if mode in ['sftp', 'rsync', 'sftp_upload']:
                speed_str = "-- MB/s"

        return Text.from_markup(f"{self.label}:[{self.style}]{speed_str:>10}[/]")

# --- Custom Renderable Classes ---

class TextSparkline:
    """Renders a sparkline using block characters."""
    def __init__(self, data: List[float], width: int = 50, height: int = 1, min_val: float = None, max_val: float = None):
        self.data = data
        self.width = width
        self.height = height
        self.min_val = min_val
        self.max_val = max_val
        # Block characters from empty to full
        self.BARS = "\u2581▂▃▄▅▆▇█"

    def __rich__(self) -> Text:
        if not self.data:
            return Text("\n".join([" " * self.width] * self.height))

        data_to_plot = self.data
        if len(self.data) > self.width:
            chunk_size = len(self.data) / self.width
            data_to_plot = []
            for i in range(self.width):
                start = int(i * chunk_size)
                end = int((i + 1) * chunk_size)
                if start == end:
                    end += 1
                chunk = self.data[start:end]
                # Use max for peaks, could use avg for smoother
                if chunk:
                    data_to_plot.append(max(chunk))
                else:
                    data_to_plot.append(0)

        min_v = min(data_to_plot) if self.min_val is None else self.min_val
        max_v = max(data_to_plot) if self.max_val is None else self.max_val

        # Fix flat line cases
        if max_v == min_v:
             # If zero, show baseline at bottom
             if max_v == 0:
                 rows = [" " * len(data_to_plot)] * (self.height - 1)
                 rows.append(self.BARS[0] * len(data_to_plot)) # Bottom row baseline
                 return Text("\n".join(rows), style="green")
             else:
                 # Middle line? For simplicity, treat as full.
                 pass

        result_rows = [""] * self.height
        range_v = max_v - min_v
        if range_v <= 0: range_v = 1.0

        for val in data_to_plot:
            val = max(min_v, min(val, max_v))
            normalized = (val - min_v) / range_v
            
            # Map normalized (0.0-1.0) to total available eighth-blocks
            # Total steps = height * 8.
            # We want strict scaling.
            # Example: height=2. steps=16. 
            # 1.0 -> 16 (full). 0.0 -> 0 (empty).
            
            total_levels = int(normalized * (self.height * 8 - 1))
            
            for r in range(self.height):
                # r=0 is TOP row, r=height-1 is BOTTOM row (common expectation in list building? No, let's be explicit)
                # Let's index 0 as BOTTOM row for calculation, then reverse for display.
                
                # This row represents levels from (r * 8) to ((r + 1) * 8)
                row_floor = r * 8
                row_ceil = (r + 1) * 8
                
                level_in_row = total_levels - row_floor
                
                char = " "
                if level_in_row <= 0:
                    char = " " # Empty (or baseline logic if needed, but handled by 0 check usually)
                    if r == 0 and total_levels == 0: char = self.BARS[0] # Ensure explicitly 0 values have baseline
                elif level_in_row >= 8:
                    char = "█"
                else:
                    char = self.BARS[level_in_row]
                
                result_rows[r] += char
        
        # Reverse rows so index 0 (bottom) is last in the list
        return Text("\n".join(reversed(result_rows)), style="green")

class _StatsPanel:
    """A renderable class for the Stats and Recent Completions panels."""
    def __init__(self, ui_manager: "UIManagerV2"):
        self.ui_manager = ui_manager

    def __rich_console__(self, console: Console, options: Any) -> RenderResult:
        with self.ui_manager._lock:
            stats = self.ui_manager._stats
            recent_completions = self.ui_manager._recent_completions
            config = self.ui_manager._layout_config
            elapsed = time.time() - stats["start_time"]

            current_dl_speed = stats.get("current_dl_speed", 0.0)
            current_ul_speed = stats.get("current_ul_speed", 0.0)
            avg_speed_hist = (sum(self.ui_manager._dl_speed_history) + sum(self.ui_manager._ul_speed_history)) / \
                            (len(self.ui_manager._dl_speed_history) + len(self.ui_manager._ul_speed_history)) if \
                            (self.ui_manager._dl_speed_history or self.ui_manager._ul_speed_history) else 1

            stats_table = Table.grid(padding=(1, 0))
            stats_table.add_column(style="bold cyan", justify="right", no_wrap=True)
            stats_table.add_column()

            transferred_gb = stats['transferred_bytes'] / (1024**3)
            total_gb = stats['total_bytes'] / (1024**3)
            remaining_gb = max(0, total_gb - transferred_gb)

            stats_table.add_row("📊 Progress: ", f"[white]{transferred_gb:.2f}/{total_gb:.2f} GB ({remaining_gb:.2f} GB rem.)[/]")
            dl_str = f"{current_dl_speed / (1024**2):.1f}"
            ul_str = f"{current_ul_speed / (1024**2):.1f}"

            # Conditionally show '--' for UL speed on non-uploading modes
            if current_ul_speed == 0 and self.ui_manager.transfer_mode in ['sftp', 'rsync', 'sftp_upload']:
                ul_str = "--"

            stats_table.add_row("⚡ Speed: ", f"[green]DL:{dl_str}[/] [yellow]UL:{ul_str}[/] MB/s")
            stats_table.add_row("📉 Avg Speed: ", f"[dim]{avg_speed_hist / (1024**2):.1f} MB/s[/]")
            stats_table.add_row("🏆 Peak Speed: ", f"[bold yellow]{stats['peak_speed'] / (1024**2):.1f} MB/s[/]")

            # Detailed counts
            stats_table.add_row("🔄 Active: ", f"[white]{stats['active_transfers']}[/]")
            stats_table.add_row("✅ Completed: ", f"[green]{stats['completed_transfers']}[/]")
            stats_table.add_row("❌ Failed: ", f"[red]{stats['failed_transfers']}[/]")

            total_files = sum(t.get('total_files', 0) for t in self.ui_manager._torrents.values())
            completed_files = sum(t.get('completed_files', 0) for t in self.ui_manager._torrents.values())
            stats_table.add_row("📂 Files: ", f"[white]{completed_files}/{total_files}[/]")

            # Compacted Time
            h, rem = divmod(elapsed, 3600); m, s = divmod(rem, 60)
            time_str = f"{int(h):02d}:{int(m):02d}:{int(s):02d}"
            eta_str = "-:--"
            if remaining_gb > 0 and avg_speed_hist > 0:
                eta_s = (remaining_gb * 1024**3) / avg_speed_hist
                eta_h, eta_rem = divmod(eta_s, 3600); eta_m, _ = divmod(eta_rem, 60)
                eta_str = f"{int(eta_h):02d}:{int(eta_m):02d}"
            stats_table.add_row("⏱️ Time: ", f"[dim]E:{time_str}[/] [cyan]ETA:{eta_str}[/]")

            # Create recent completions table (if space allows)
            max_recent = config.get("recent_completions", 0)
            if recent_completions and max_recent > 0:
                recent_table = Table.grid(padding=(0, 1))
                recent_table.add_column(style="dim", no_wrap=True)
                recent_table.add_column(style="dim")
                for name, size, duration in list(recent_completions)[-max_recent:]:
                    display_name = smart_truncate(name, 30)
                    speed = size / duration if duration > 0 else 0
                    recent_table.add_row(f"✓ {display_name}", f"{speed / (1024**2):.1f} MB/s")

                yield Group(
                    Panel(stats_table, title="[bold cyan]📊 Statistics", border_style="dim", style="none"),
                    Panel(recent_table, title="[bold green]🎉 Recent Completions", border_style="dim", style="on #16213e")
                )
            else:
                yield Panel(stats_table, title="[bold cyan]📊 Statistics", border_style="dim", style="none")

class _ActivityPanel:
    """Renders the Sparkline in a dedicated panel."""
    def __init__(self, ui_manager):
        self.ui_manager = ui_manager

    def __rich_console__(self, console, options):
        with self.ui_manager._lock:
            # Retrieve history from Task 2's implementation
            # Retrieve history from Task 2's implementation
            history = getattr(self.ui_manager, "_speed_history_data", [])

            # Fallback to internal history if external history is not provided
            if not history:
                 dl_hist = list(self.ui_manager._dl_speed_history)
                 ul_hist = list(self.ui_manager._ul_speed_history)
                 # Make them equal length
                 max_len = max(len(dl_hist), len(ul_hist)) if dl_hist or ul_hist else 0
                 if max_len > 0:
                     # Pad with zeros at the beginning
                     dl_hist = [0.0] * (max_len - len(dl_hist)) + dl_hist
                     ul_hist = [0.0] * (max_len - len(ul_hist)) + ul_hist
                     history = [d + u for d, u in zip(dl_hist, ul_hist)]

            # Stabilize Y-axis: Use session peak (min 1 MB/s) as max_val
            peak = self.ui_manager._stats.get("peak_speed", 0.0)
            graph_max = max(peak, 1024 * 1024) # Minimum 1 MB/s scale

            # Use the TextSparkline class from Task 2
            # options.max_width helps size it dynamically
            sparkline = TextSparkline(
                history,
                width=options.max_width - 4,
                height=8,
                min_val=0,
                max_val=graph_max
            )

        yield Panel(
            sparkline,
            title="[bold cyan]Network Activity (5m)[/]",
            border_style="dim",
            style="none", # Transparent
            height=10,
        )

class _ActiveTorrentsPanel:
    """A renderable class for the Active Torrents list."""
    def __init__(self, ui_manager: "UIManagerV2"):
        self.ui_manager = ui_manager

    def _render_progress_bar(self, percent: float, width: int = 10) -> Text:
        """Creates a rich Text progress bar."""
        filled_width = int(percent / 100 * width)
        bar_char = "█"
        empty_char = "─"
        bar = bar_char * filled_width + empty_char * (width - filled_width)
        style = "green" if percent >= 100 else "blue"
        return Text.from_markup(f"[[{style}]{bar}[/]] {percent:>3.0f}%")

    def _render_file_progress_bar(self, percent: float, width: int = 15) -> Text:
        """Creates a small rich Text progress bar for files."""
        filled_width = int(percent / 100 * width)
        bar_char = "█"
        empty_char = "─"
        bar = bar_char * filled_width + empty_char * (width - filled_width)
        style = "green" if percent >= 100 else "blue"
        return Text.from_markup(f" [[{style}]{bar}[/]]")

    def __rich_console__(self, console: Console, options: Any) -> RenderResult:
        with self.ui_manager._lock:
            torrents = self.ui_manager._torrents
            stats = self.ui_manager._stats
            config = self.ui_manager._layout_config

            table = Table.grid(padding=(0, 1), expand=True)
            # Use adaptive columns
            if config["show_progress_bars"]:
                table.add_column(style="bold", no_wrap=True, width=18) # Progress bar
            else:
                table.add_column(style="bold", no_wrap=True, width=5) # "XX%"
            table.add_column() # Name & File List

            active_count = 0
            for hash_, torrent in torrents.items():
                if torrent["status"] == "transferring":
                    active_count += 1
                    name = torrent["name"]
                    display_name = smart_truncate(name, config["torrent_name_width"])
                    progress = torrent["transferred"] / torrent["size"] * 100 if torrent["size"] > 0 else 0

                    # --- ADD THIS LINE (Fix for 106%) ---
                    progress = min(progress, 100.0) # AI-INVARIANT
                    # --- END ADDITION ---

                    is_repair = torrent.get("is_repair", False)

                    # Build file list
                    file_renderables: List[Text] = [] # Changed from file_lines
                    files = self.ui_manager._file_status.get(hash_, {})

                    # Sort files: active, failed, queued, completed
                    def sort_key(item):
                        status = item[1] if isinstance(item[1], str) else "queued"
                        if status in ["downloading", "uploading"]: return 0
                        if status == "failed": return 1
                        if status == "queued": return 2
                        return 3 # completed

                    sorted_files = sorted(files.items(), key=sort_key)
                    max_files = min(7, config.get("terminal_height", 100) // 10)

                    for file_path, status in sorted_files[:max_files]:
                        file_name = smart_truncate(file_path.split('/')[-1], config["file_name_width"])
                        progress_display = Text("")

                        file_progress_data = self.ui_manager._file_progress.get(hash_, {}).get(file_path)
                        if file_progress_data and status in ["downloading", "uploading"]:
                            transferred, total = file_progress_data
                            file_percent = (transferred / total * 100) if total > 0 else 0
                            if config["show_progress_bars"]:
                                progress_display = self._render_file_progress_bar(file_percent)
                            else:
                                progress_display = Text(f" {file_percent:>3.0f}%")

                        # Append progress to the correct style of text
                        style_map = {
                            "downloading": f" [blue]⇩ {file_name}[/blue]",
                            "uploading": f" [yellow]⇧ {file_name}[/yellow]",
                            "failed": f" [bold red]✖ {file_name}[/bold red]",
                            "completed": f" [dim]✓ {file_name}[/dim]",
                            "queued": f" [dim]· {file_name}[/dim]"
                        }

                        if is_repair and status == "downloading":
                             style_map["downloading"] = f" [bold magenta]🔍 {file_name}[/bold magenta]"

                        file_text = Text.from_markup(style_map.get(status, f" [dim]· {file_name}[/dim]"))
                        file_text.append(progress_display)
                        file_renderables.append(file_text)


                    if len(files) > max_files:
                        file_renderables.append(Text.from_markup(f" [dim]... and {len(files) - max_files} more.[/dim]"))

                    files_panel_content = Text("\n").join(file_renderables)
                    completed_files = torrent.get('completed_files', 0)
                    total_files = torrent.get('total_files', 0)

                    # --- START MODIFICATION (Fix for rsync display) ---
                    mode = self.ui_manager.transfer_mode
                    if 'rsync' in mode:
                        size_gb = torrent["size"] / (1024**3)
                        transferred_gb = torrent["transferred"] / (1024**3)
                        # Clamp transferred_gb to not exceed size_gb in the display
                        transferred_gb = min(transferred_gb, size_gb)
                        files_display_str = f"({transferred_gb:.2f} / {size_gb:.2f} GB)"
                    else:
                        files_display_str = f"({completed_files}/{total_files} files)"
                    # --- END MODIFICATION ---

                    # Main torrent entry with adaptive progress display
                    progress_display = self._render_progress_bar(progress) if config["show_progress_bars"] else f"{progress:>3.0f}%"

                    name_style = "bold magenta" if is_repair else "bold cyan"
                    repair_label = " (🔍 Repairing...)" if is_repair else ""

                    table.add_row(
                        progress_display,
                        Group(
                            # --- UPDATE THIS LINE ---
                            Text.from_markup(f"[{name_style}]{display_name}[/{name_style}]{repair_label} [dim]{files_display_str}[/dim]"),
                            # --- END UPDATE ---
                            files_panel_content
                        )
                    )
                    table.add_row("", "") # Spacer

            if active_count == 0:
                table.add_row("[dim]⏸️", "[dim]Waiting for torrents...[/dim]")

            # Add queued count
            queued = stats["total_torrents"] - stats["completed_transfers"] - stats["failed_transfers"] - active_count
            if queued > 0:
                table.add_row("", "")
                table.add_row("[bold]⏳", f"[yellow]Queued: {queued} torrent(s)[/yellow]")

            yield Panel(
                table,
                title="[bold yellow]🎯 Active Torrents",
                border_style="dim",
                style="on #16213e"
            )

class _LogPanel:
    """A renderable class for the Live Log."""
    def __init__(self, ui_manager: "UIManagerV2"):
        self.ui_manager = ui_manager

    def __rich_console__(self, console: Console, options: Any) -> RenderResult:
        with self.ui_manager._lock:
            if self.ui_manager._current_status:
                display_content = Align.center(Text(self.ui_manager._current_status, style="bold green"))
            else:
                display_content = Align.left(Text("\n").join(list(self.ui_manager._log_buffer)))

            yield Panel(
                display_content,
                title="[bold]📜 Live Log",
                border_style="dim",
                style="on #0a0e27"
            )

class UMLoggingHandler(logging.Handler):
    """A custom logging handler that forwards log records to the UIManager.

    This handler is attached to the root logger when the Rich UI is active. It
    captures log messages from other parts of the application and calls the
    `UIManagerV2.log()` method, which adds the formatted message to the UI's
    live log panel.

    Attributes:
        ui_manager: An instance of the UIManagerV2 to which logs will be sent.
    """
    def __init__(self, ui_manager: "UIManagerV2"):
        """Initializes the UMLoggingHandler.

        Args:
            ui_manager: The UIManagerV2 instance to use for logging.
        """
        super().__init__()
        self.ui_manager = ui_manager

    def emit(self, record: logging.LogRecord):
        if "torrent_mover.ui" in record.name:
            return
        # The UI's log method will handle timestamps and formatting
        self.ui_manager.log(record.getMessage())

class BaseUIManager(abc.ABC):
    """Defines the interface for all UI manager implementations.

    This abstract base class ensures that any UI implementation (e.g., a rich
    terminal UI or a simple logging-based UI) provides a consistent set of
    methods for the main application logic to call. This allows the UI to be
    swapped without changing the core transfer logic.
    """
    def __init__(self, watchdog_timeout: int = 1500):
        self.watchdog = TransferWatchdog(timeout_seconds=watchdog_timeout)

    def __enter__(self):
        """Enters the context manager, preparing the UI for display."""
        self.watchdog.start(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exits the context manager, cleaning up UI resources."""
        self.watchdog.stop()
        pass

    @abc.abstractmethod
    def set_transfer_mode(self, mode: str):
        pass

    @abc.abstractmethod
    def set_analysis_total(self, total: int):
        pass

    @abc.abstractmethod
    def advance_analysis(self):
        pass

    @abc.abstractmethod
    def complete_analysis(self):
        pass

    @abc.abstractmethod
    def set_overall_total(self, total_bytes: float):
        pass

    @abc.abstractmethod
    def start_torrent_transfer(self, torrent_hash: str, torrent_name: str, total_size: float, all_files: List[str], transfer_multiplier: int = 1, is_repair: bool = False):
        pass

    @abc.abstractmethod
    def update_torrent_progress(self, torrent_hash: str, bytes_transferred: float, transfer_type: str):
        pass

    @abc.abstractmethod
    def start_file_transfer(self, torrent_hash: str, file_path: str, status: str):
        pass

    @abc.abstractmethod
    def complete_file_transfer(self, torrent_hash: str, file_path: str):
        pass

    @abc.abstractmethod
    def fail_file_transfer(self, torrent_hash: str, file_path: str):
        pass

    @abc.abstractmethod
    def complete_torrent_transfer(self, torrent_hash: str, success: bool = True):
        pass

    @abc.abstractmethod
    def log(self, message: str):
        pass

    @abc.abstractmethod
    def set_final_status(self, message: str):
        pass

    @abc.abstractmethod
    def display_stats(self, stats: Dict[str, Any]) -> None:
        pass

    @abc.abstractmethod
    def pet_watchdog(self):
        """Signals that a non-transfer activity has occurred."""
        self.watchdog.pet()
        pass


class SimpleUIManager(BaseUIManager):
    """A non-interactive UI that logs progress to the console via `logging`.

    This UI manager is suitable for environments where a rich, interactive
    display is not feasible, such as in `tmux`, `screen`, or non-interactive
    SSH sessions. It implements the `BaseUIManager` interface by writing
    status updates to the standard Python `logging` module.
    """
    def __init__(self, watchdog_timeout: int = 1500):
        """Initializes the SimpleUIManager."""
        super().__init__(watchdog_timeout)
        self._stats = {
            "total_torrents": 0,
            "total_bytes": 0,
            "transferred_bytes": 0,
            "start_time": time.time(),
            "completed_transfers": 0,
            "failed_transfers": 0,
        }
        logging.info("Using simple UI (standard logging).")

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Logs a final message upon exiting."""
        super().__exit__(exc_type, exc_val, exc_tb)
        if exc_type:
            logging.error(f"An error occurred: {exc_val}")
        logging.info("--- Torrent Mover script finished ---")

    def set_transfer_mode(self, mode: str):
        """Logs the configured transfer mode."""
        logging.info(f"Transfer Mode: {mode.upper()}")

    def log(self, message: str):
        """Logs a message, stripping any Rich markup."""
        # Strip rich markup
        message = re.sub(r"\[.*?\]", "", message)
        logging.info(message)

    def set_analysis_total(self, total: int):
        """Logs the total number of torrents found for analysis."""
        self._stats["total_torrents"] = total
        logging.info(f"Analysis: Found {total} torrents to process.")

    def advance_analysis(self):
        """A no-op for the simple UI."""
        pass # Not needed for simple logger

    def complete_analysis(self):
        """Logs the completion of the analysis phase."""
        logging.info("Analysis: Complete.")

    def set_overall_total(self, total_bytes: float):
        """Logs the total size of all transfers."""
        self._stats["total_bytes"] = total_bytes
        logging.info(f"Transfer: Total size: {total_bytes / (1024**3):.2f} GB")

    def start_torrent_transfer(self, torrent_hash: str, torrent_name: str, total_size: float, all_files: List[str], transfer_multiplier: int = 1, is_repair: bool = False):
        """Logs the start of a new torrent transfer."""
        if is_repair:
            logging.info(f"Starting REPAIR transfer: {torrent_name}")
        else:
            logging.info(f"Starting Transfer: {torrent_name}")

    def update_torrent_progress(self, torrent_hash: str, bytes_transferred: float, transfer_type: str):
        """A no-op to avoid overly verbose logging."""
        # We don't log every progress update in simple mode, too noisy.
        pass

    def start_file_transfer(self, torrent_hash: str, file_path: str, status: str):
        """Logs the start of an individual file transfer at the DEBUG level."""
        logging.debug(f"File Transfer: [{status.upper()}] {file_path}")

    def complete_file_transfer(self, torrent_hash: str, file_path: str):
        """Logs the completion of a file transfer at the DEBUG level."""
        logging.debug(f"File Transfer: [COMPLETED] {file_path}")

    def fail_file_transfer(self, torrent_hash: str, file_path: str):
        """Logs the failure of a file transfer as a warning."""
        logging.warning(f"File Transfer: [FAILED] {file_path}")

    def complete_torrent_transfer(self, torrent_hash: str, success: bool = True):
        """Logs the completion or failure of a torrent's transfer."""
        if success:
            self._stats["completed_transfers"] += 1
            logging.info(f"Transfer Complete: Torrent hash {torrent_hash[:10]}...")
        else:
            self._stats["failed_transfers"] += 1
            logging.error(f"Transfer Failed: Torrent hash {torrent_hash[:10]}...")

    def set_final_status(self, message: str):
        """Logs a final status message."""
        logging.info(f"Status: {message}")

    def pet_watchdog(self):
        """(Simple UI) No-op."""
        super().pet_watchdog()
        pass

    def display_stats(self, stats: Dict[str, Any]) -> None:
        """Displays the final transfer statistics."""
        # This is called by UIManagerV2, but SimpleUIManager logs at the end.
        logging.info("--- Final Statistics ---")
        logging.info(f"Total Bytes Transferred: {self._stats['transferred_bytes'] / (1024**3):.2f} GB")
        logging.info(f"Successful Transfers: {self._stats['completed_transfers']}")
        logging.info(f"Failed Transfers: {self._stats['failed_transfers']}")
        logging.info(f"Total Duration: {time.time() - self._stats['start_time']:.2f} seconds")


class UIManagerV2(BaseUIManager):
    """A rich, interactive terminal UI powered by the `rich` library.

    This class provides a real-time dashboard for monitoring the torrent
    transfer process. It includes:
    -   Overall progress bars for analysis and transfers.
    -   A live-updating panel with detailed statistics (speeds, counts, ETA).
    -   A panel showing currently active torrents and their individual file progress.
    -   A live log panel that captures log messages from the application.

    It uses a background thread to periodically update statistics and relies on
    custom `rich` renderable classes to display the data in a structured layout.
    """

    def __init__(self, version: str = "", rich_handler: Optional[logging.Handler] = None, watchdog_timeout: int = 1500):
        """Initializes the UIManagerV2.

        Args:
            version: The application version string, displayed in the header.
            rich_handler: A reference to the RichHandler, which is temporarily
                removed and replaced by the UI's internal handler during display.
        """
        super().__init__(watchdog_timeout)
        self.console = Console()
        self._lock = threading.RLock()
        self._live: Optional[Live] = None
        self.version = version
        self._rich_handler_ref = rich_handler
        self._um_log_handler = UMLoggingHandler(self)
        self.transfer_mode = "" # For transfer mode
        self._responsive_layout = ResponsiveLayout(self.console)
        self._layout_config = self._responsive_layout.get_layout_config()
        self._log_buffer: Deque[Text] = deque(maxlen=self._layout_config["log_lines"]) # For log panel
        self._current_header_string_template: str = ""
        self._last_header_text_part: str = ""
        self._current_status: Optional[str] = None
        self._analysis_complete = False # Add this flag

        # Data structures
        self._torrents: OrderedDict[str, Dict] = OrderedDict()
        self._active_torrents: Deque[str] = deque(maxlen=5)
        self._completed_hashes: set = set()
        self._recent_completions: Deque[tuple] = deque(maxlen=5) # (name, size, duration)

        # Data structures for file-level tracking
        self._file_lists: Dict[str, List[str]] = {} # torrent_hash -> [file_name_1, file_name_2, ...]
        self._file_status: Dict[str, Dict[str, str]] = {} # torrent_hash -> {file_name -> "queued" | "downloading" | "uploading" | "completed"}
        self._file_progress: Dict[str, Dict[str, Tuple[int, int]]] = {} # torrent_hash -> {file_name -> (transferred, total)}


        # Statistics
        self._stats = {
            "last_activity_timestamp": time.monotonic(), # <-- ADD THIS
            "total_torrents": 0,
            "total_bytes": 0,
            "transferred_bytes": 0,
            "start_time": time.time(),
            "active_transfers": 0,
            "completed_transfers": 0,
            "failed_transfers": 0,
            "peak_speed": 0.0,
            "current_dl_speed": 0.0,
            "current_ul_speed": 0.0,
            "last_dl_bytes": 0,
            "last_ul_bytes": 0,
            "transferred_dl_bytes": 0,
            "transferred_ul_bytes": 0,
            "last_dl_speed_check": time.time(),
            "last_ul_speed_check": time.time(),
        }
        self._dl_speed_history = deque(maxlen=300)
        self._ul_speed_history = deque(maxlen=300)
        self._speed_history_data: List[float] = []


        # Create layout with better proportions
        self.layout = Layout()
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="body", ratio=65),
            Layout(name="footer", ratio=25)
        )

        # Split body into two columns
        self.layout["body"].split_row(
            Layout(name="left", ratio=70),  # Active Torrents
            Layout(name="right", ratio=30) # Stats
        )

        # Initialize components
        self._setup_header()
        self._setup_progress()
        self._setup_stats_panel()
        self._setup_footer()

    def _setup_header(self):
        """Enhanced header with version and mode indicators."""
        mode_str = f"[dim]({self.transfer_mode.upper()})[/dim]" if self.transfer_mode else ""
        header_content = f"🚀 Torrent Mover v{self.version} {mode_str} - [green]Initializing...[/]"
        self.layout["header"].update(
            Panel(
                Align.center(header_content),
                title="[bold magenta]TORRENT MOVER[/]",
                border_style="dim",
                style="on #1a1a2e"
            )
        )
        self._current_header_string_template = "🚀 Torrent Mover v{version} {mode_str} - {text}"
        self._last_header_text_part = "[green]Initializing...[/]"

    def _setup_progress(self):
        """Setup progress bars with better formatting. FIXED: Use custom column."""
        config = self._layout_config

        columns = [
            TextColumn("[bold]{task.description}", justify="left"),
            BarColumn(bar_width=None, complete_style="green", finished_style="bold green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            "•",
            DownloadColumn(binary_units=True),
        ]

        if config["show_speed"]:
            columns.extend([
                "•",
                _SpeedColumn("current_dl_speed", "DL", "green", ui_manager=self),
                "•",
                _SpeedColumn("current_ul_speed", "UL", "yellow", ui_manager=self),
            ])

        if config["show_eta"]:
            columns.extend([
                "•",
                TimeRemainingColumn(),
            ])

        self.main_progress = Progress(*columns, expand=True)

        self.analysis_task = self.main_progress.add_task(
            "[cyan]📊 Analysis", total=100, visible=False
        )

        self.overall_task = self.main_progress.add_task(
            "[green]📦 Overall Progress",
            total=100,
            visible=False
        )

        # Combine into left panel
        left_group = Group(
            Panel(
                self.main_progress,
                title="[bold green]📈 Transfer Progress",
                border_style="dim",
                style="on #16213e"
            ),
            _ActiveTorrentsPanel(self), # Add the new renderable panel
            _ActivityPanel(self)  # <--- NEW POSITION
        )
        self.layout["left"].update(left_group)

    def _setup_stats_panel(self):
        """Setup stats panel using the renderable class."""
        self.layout["right"].update(_StatsPanel(self))

    def _setup_footer(self):
        """Initial setup for the log panel footer."""
        self.layout["footer"].update(_LogPanel(self))

    def __enter__(self):
        super().__enter__()
        root_logger = logging.getLogger()
        root_logger.addHandler(self._um_log_handler)
        self._live = Live(
            self.layout,
            console=self.console,
            screen=True,
            redirect_stderr=False,
            refresh_per_second=10
        )
        self._live.start()

        self._stats_thread_stop = threading.Event()
        self._stats_thread = threading.Thread(target=self._stats_updater, daemon=True)
        self._stats_thread.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        if self._live:
            self._stats_thread_stop.set()
            # Join the thread with a timeout
            self._stats_thread.join(timeout=5.0)
            # Check if the thread is still alive
            if self._stats_thread.is_alive():
                logging.warning("Stats thread did not stop cleanly and will be left running.")
            # Ensure progress bars and live display are stopped
            try:
                self.main_progress.stop()
            except Exception as e:
                logging.error(f"Error stopping main progress: {e}")

            try:
                self._live.stop()
            except Exception as e:
                logging.error(f"Error stopping live display: {e}")

        root_logger = logging.getLogger()
        root_logger.removeHandler(self._um_log_handler)
        if self._rich_handler_ref:
            root_logger.addHandler(self._rich_handler_ref)

    def set_final_status(self, message: str):
        """Displays a final, persistent status message in the log panel.

        This is used to show a summary message (e.g., "All tasks finished.")
        after the live updates have stopped.

        Args:
            message: The final status message to display.
        """
        with self._lock:
            self._current_status = message

    def _check_and_update_layout(self):
        """Checks for terminal width changes and updates the layout accordingly."""
        new_config = self._responsive_layout.get_layout_config()
        if new_config["terminal_width"] != self._layout_config["terminal_width"]:
            self._layout_config = new_config
            # Update layout ratios
            self.layout["body"].split_row(
                Layout(name="left", ratio=new_config["left_ratio"]),
                Layout(name="right", ratio=new_config["right_ratio"])
            )
            # Update log buffer size, preserving recent logs
            self._log_buffer = deque(list(self._log_buffer), maxlen=new_config["log_lines"])

    def _stats_updater(self):
        """Background thread to update stats and progress bars."""
        last_layout_check = time.time()

        while not self._stats_thread_stop.wait(1.0):
            if time.time() - last_layout_check > 5.0:
                with self._lock:
                    self._check_and_update_layout()
                last_layout_check = time.time()

            with self._lock:
                # --- Calculate Speeds ---
                current_dl_speed = 0.0
                current_ul_speed = 0.0
                time_since_last_dl = time.time() - self._stats["last_dl_speed_check"]
                time_since_last_ul = time.time() - self._stats["last_ul_speed_check"]

                if time_since_last_dl >= 1.0:
                    bytes_since_last = max(0, self._stats["transferred_dl_bytes"] - self._stats["last_dl_bytes"])
                    current_dl_speed = bytes_since_last / time_since_last_dl
                    
                    self._stats["current_dl_speed"] = current_dl_speed
                    self._dl_speed_history.append(current_dl_speed)
                    
                    self._stats["last_dl_bytes"] = self._stats["transferred_dl_bytes"]
                    self._stats["last_dl_speed_check"] = time.time()
                else:
                    current_dl_speed = self._stats.get("current_dl_speed", 0.0)

                if time_since_last_ul >= 1.0:
                    bytes_since_last = max(0, self._stats["transferred_ul_bytes"] - self._stats["last_ul_bytes"])
                    current_ul_speed = bytes_since_last / time_since_last_ul
                    
                    self._stats["current_ul_speed"] = current_ul_speed
                    self._ul_speed_history.append(current_ul_speed)
                    
                    self._stats["last_ul_bytes"] = self._stats["transferred_ul_bytes"]
                    self._stats["last_ul_speed_check"] = time.time()
                else:
                    current_ul_speed = self._stats.get("current_ul_speed", 0.0)

                # FIX 5: Update peak speed only if we have current activity
                current_total_speed = current_dl_speed + current_ul_speed
                if current_total_speed > self._stats.get("peak_speed", 0.0):
                    self._stats["peak_speed"] = current_total_speed

                # --- Update Averages ---
                # --- Update Averages ---
                # REMOVED: Manual popping to allow history to grow to maxlen=300 for the graph.

                avg_dl_speed = sum(self._dl_speed_history) / len(self._dl_speed_history) if self._dl_speed_history else 0.0
                avg_ul_speed = sum(self._ul_speed_history) / len(self._ul_speed_history) if self._ul_speed_history else 0.0

                self._stats["avg_dl_speed"] = avg_dl_speed
                self._stats["avg_ul_speed"] = avg_ul_speed

                # --- Update UI Elements ---
                if self._stats_table:
                    self._update_stats_table()

                if self._torrent_progress_table:
                    self._update_torrent_progress_table()

    # ===== Public API (keeping existing methods) =====

    def update_current_speed(self, download_speed: float, upload_speed: float = 0.0):
        """Overrides internal speed calculation with external monitor data."""
        with self._lock:
            self._stats["current_dl_speed"] = download_speed
            self._stats["current_ul_speed"] = upload_speed
            # Important: Update the history queues used by the internal logic
            # so Avg/Peak stats remain consistent if they use these queues.
            self._dl_speed_history.append(download_speed)
            if upload_speed > 0:
                self._ul_speed_history.append(upload_speed)
            
            # Ensure peak speed is updated when using external monitors
            total_speed = download_speed + upload_speed
            if total_speed > self._stats.get("peak_speed", 0.0):
                self._stats["peak_speed"] = total_speed

    def update_speed_history(self, history: List[float]):
        with self._lock:
            self._speed_history_data = history

    def set_transfer_mode(self, mode: str):
        """Sets and displays the current transfer mode in the UI header."""
        with self._lock:
            self.transfer_mode = mode
            self._setup_header()

    def set_analysis_total(self, total: int):
        """Initializes the analysis progress bar with the total number of torrents."""
        with self._lock:
            self._stats["total_torrents"] = total
            self.main_progress.update(self.analysis_task, total=total, visible=True)

    def advance_analysis(self):
        """Advances the analysis progress bar by one step."""
        self.main_progress.update(self.analysis_task, advance=1)

    def complete_analysis(self):
        """Hides the analysis progress bar upon completion."""
        with self._lock:
            self._analysis_complete = True
            try:
                self.main_progress.update(self.analysis_task, visible=False)
            except Exception as e:
                logging.error(f"Error hiding analysis task: {e}")

    def set_overall_total(self, total_bytes: float):
        """Initializes the overall transfer progress bar with the total size."""
        with self._lock:
            self._stats["total_bytes"] = total_bytes
            self.main_progress.update(self.overall_task, total=total_bytes, visible=True)

    def start_torrent_transfer(self, torrent_hash: str, torrent_name: str, total_size: float, all_files: List[str], transfer_multiplier: int = 1, is_repair: bool = False):
        """Adds a new torrent to the UI, making it visible in the 'Active Torrents' panel."""
        with self._lock:
            total_files = len(all_files)
            self._torrents[torrent_hash] = {
                "name": torrent_name,
                "size": total_size * transfer_multiplier,
                "total_files": total_files,
                "completed_files": 0,
                "transferred": 0,
                "status": "transferring",
                "start_time": time.time(),
                "bytes_for_delta_calc": 0, # <-- ADD THIS LINE
                "is_repair": is_repair,
            }
            self._file_lists[torrent_hash] = all_files
            self._file_status[torrent_hash] = {file_name: "queued" for file_name in all_files}
            self._active_torrents.append(torrent_hash)
            self._stats["active_transfers"] += 1

    def update_torrent_progress(self, torrent_hash: str, bytes_transferred: float, transfer_type: str):
        """Updates the progress for a torrent and the overall progress bar.

        # AI-CONTEXT: Delta Calculation
        # We calculate the delta since the last update to drive the overall progress bar.

        Args:
            torrent_hash: The hash of the torrent to update.
            bytes_transferred: The number of bytes to add to the progress.
            transfer_type: Either "download" or "upload", for stat tracking.
        """
        with self._lock:
            if torrent_hash in self._torrents:
                self._torrents[torrent_hash]["transferred"] += bytes_transferred
                if transfer_type == "download":
                    self._stats["transferred_dl_bytes"] = self._stats.get("transferred_dl_bytes", 0) + bytes_transferred
                elif transfer_type == "upload":
                    self._stats["transferred_ul_bytes"] = self._stats.get("transferred_ul_bytes", 0) + bytes_transferred
                self._stats["transferred_bytes"] = self._stats.get("transferred_dl_bytes", 0) + self._stats.get("transferred_ul_bytes", 0)
            self.main_progress.update(self.overall_task, advance=bytes_transferred)

    def start_file_transfer(self, torrent_hash: str, file_path: str, status: str):
        """Sets the status of an individual file within a torrent (e.g., "downloading")."""
        with self._lock:
            if torrent_hash in self._file_status and file_path in self._file_status[torrent_hash]:
                self._file_status[torrent_hash][file_path] = status

    def complete_file_transfer(self, torrent_hash: str, file_path: str):
        """Marks an individual file as completed."""
        with self._lock:
            if torrent_hash in self._file_status and file_path in self._file_status[torrent_hash]:
                if self._file_status[torrent_hash][file_path] != "completed":
                    self._file_status[torrent_hash][file_path] = "completed"
                    # FIX: More explicit handling
                    if torrent_hash in self._torrents:
                        current_completed = self._torrents[torrent_hash].get("completed_files", 0)
                        total_files = self._torrents[torrent_hash].get("total_files", 0)
                        # Prevent completed from exceeding total
                        if current_completed < total_files:
                            self._torrents[torrent_hash]["completed_files"] = current_completed + 1

    def fail_file_transfer(self, torrent_hash: str, file_path: str):
        """Marks an individual file as failed."""
        with self._lock:
            if torrent_hash in self._file_status and file_path in self._file_status[torrent_hash]:
                self._file_status[torrent_hash][file_path] = "failed"

    def complete_torrent_transfer(self, torrent_hash: str, success: bool = True):
        """Finalizes a torrent's status, moving it out of the active list."""
        with self._lock:
            if torrent_hash in self._torrents:
                try:
                    torrent = self._torrents[torrent_hash]
                    torrent["status"] = "completed" if success else "failed"
                    self._completed_hashes.add(torrent_hash)
                    self._stats["active_transfers"] = max(0, self._stats["active_transfers"] - 1)
                    if success:
                        self._stats["completed_transfers"] += 1
                        duration = time.time() - torrent.get("start_time", time.time())
                        self._recent_completions.append((
                            torrent["name"],
                            torrent["size"],
                            duration
                        ))
                    else:
                        self._stats["failed_transfers"] += 1
                finally:
                    if torrent_hash in self._file_lists:
                        del self._file_lists[torrent_hash]
                    if torrent_hash in self._file_status:
                        del self._file_status[torrent_hash]

    def log(self, message: str):
        """Adds a timestamped message to the live log panel."""
        with self._lock:
            timestamp = time.strftime("%H:%M:%S")
            self._log_buffer.append(Text.from_markup(f"[{timestamp}] {message}"))

    def pet_watchdog(self):
        """Signals that a non-transfer activity has occurred."""
        super().pet_watchdog()
        with self._lock:
            self._stats["last_activity_timestamp"] = time.monotonic()

    def display_stats(self, stats: Dict[str, Any]) -> None:
        """Prints final summary statistics to the console after the UI has shut down."""
        self.console.print("--- Final Statistics ---", style="bold green")
        self.console.print(f"Total Bytes Transferred: {self._stats['transferred_bytes'] / (1024**3):.2f} GB")
        self.console.print(f"Peak Speed: {self._stats['peak_speed'] / (1024**2):.2f} MB/s")
        self.console.print(f"Successful Transfers: {self._stats['completed_transfers']}")
        self.console.print(f"Failed Transfers: {self._stats['failed_transfers']}")
        self.console.print(f"Total Duration: {time.time() - self._stats['start_time']:.2f} seconds")

__all__ = ["BaseUIManager", "SimpleUIManager", "UIManagerV2"]
