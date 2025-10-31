"""Manages the user interface, providing both rich and simple modes.

This module defines the abstract base class `BaseUIManager` to ensure a consistent
interface for UI operations. It provides two concrete implementations:

1.  `UIManagerV2`: A sophisticated, real-time terminal UI built with the `rich`
    library. It features multiple panels for progress bars, statistics, active
    transfers, and live logging.

2.  `SimpleUIManager`: A basic UI that outputs progress and status updates as
    standard log messages. It is suitable for non-interactive environments or
    terminals that do not support the rich display.
"""
import os
from rich.console import Console, Group, RenderResult
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
Progress,
TextColumn,
BarColumn,
TransferSpeedColumn,
TimeRemainingColumn,
DownloadColumn,
TaskID,
ProgressColumn,
)
from rich.table import Table
from rich.text import Text
from rich.align import Align
import logging
import threading
from collections import OrderedDict, deque
from typing import Dict, Any, Optional, Deque, Tuple, List, Type
from rich.layout import Layout
import time
import re
import abc

# --- Custom Progress Column for Speed ---

class _SpeedColumn(ProgressColumn):
    """A custom `rich` progress column to display formatted DL/UL speeds.

    This column reads pre-formatted speed strings from a task's `fields`
    dictionary, allowing for centralized speed calculation and consistent
    display across the UI.
    """
    def __init__(self, field_name: str, label: str, style: str):
        """Initializes the _SpeedColumn.

        Args:
            field_name: The key in `task.fields` where the speed string is stored.
            label: The text label to display before the speed (e.g., "DL").
            style: The `rich` style to apply to the speed value.
        """
        self.field_name = field_name
        self.label = label
        self.style = style
        self.ui_manager = ui_manager # <-- Store the UI Manager instance
        super().__init__()

    def render(self, task: "Task") -> Text:
        """Renders the speed value for the progress bar.

        Args:
            task: The `rich` Task object being rendered.

        Returns:
            A `rich.text.Text` object containing the formatted speed.
        """
        speed = task.fields.get(self.field_name, '0.00 MB/s')
        return Text.from_markup(f"{self.label}:[{self.style}]{speed:>10}[/]")

# --- Custom Renderable Classes ---

class _StatsPanel:
    """A `rich` renderable for displaying statistics and recent completions."""
    def __init__(self, ui_manager: "UIManagerV2"):
        """Initializes the _StatsPanel.

        Args:
            ui_manager: A reference to the parent `UIManagerV2` instance to
                        access shared state.
        """
        self.ui_manager = ui_manager

    def __rich_console__(self, console: Console, options: Any) -> RenderResult:
        """Renders the statistics and recent completions panels.

        This method is called by `rich` to generate the display. It builds
        tables with the latest data from the UI manager's state.

        Args:
            console: The `rich` Console object.
            options: The console options.

        Yields:
            The `rich` renderable objects to be displayed.
        """
        with self.ui_manager._lock:
            stats = self.ui_manager._stats
            recent_completions = self.ui_manager._recent_completions
            elapsed = time.time() - stats["start_time"]

            current_dl_speed = stats.get("current_dl_speed", 0.0)
            current_ul_speed = stats.get("current_ul_speed", 0.0)
            avg_speed_hist = sum(self.ui_manager._dl_speed_history) + sum(self.ui_manager._ul_speed_history)
            avg_speed_hist /= (len(self.ui_manager._dl_speed_history) + len(self.ui_manager._ul_speed_history)) if (self.ui_manager._dl_speed_history or self.ui_manager._ul_speed_history) else 1

            stats_table = Table.grid(padding=(0, 2))
            stats_table.add_column(style="bold cyan", justify="right", no_wrap=True)
            stats_table.add_column()

            transferred_gb = stats['transferred_bytes'] / (1024**3)
            total_gb = stats['total_bytes'] / (1024**3)
            remaining_gb = max(0, total_gb - transferred_gb)

            stats_table.add_row("📊 Transferred:", f"[white]{transferred_gb:.2f} / {total_gb:.2f} GB[/white]")
            stats_table.add_row("⏳ Remaining:", f"[white]{remaining_gb:.2f} GB[/white]")
            stats_table.add_row("⚡ DL Speed:", f"[green]{current_dl_speed / (1024**2):.2f} MB/s[/green]")
            stats_table.add_row("⚡ UL Speed:", f"[yellow]{current_ul_speed / (1024**2):.2f} MB/s[/yellow]")
            stats_table.add_row("📈 Avg Speed:", f"[dim]{avg_speed_hist / (1024**2):.2f} MB/s[/dim]")
            stats_table.add_row("🔥 Peak Speed:", f"[dim]{stats['peak_speed'] / (1024**2):.2f} MB/s[/dim]")
            stats_table.add_row("", "")
            stats_table.add_row("🔄 Active:", f"[white]{stats['active_transfers']}[/white]")
            stats_table.add_row("✅ Completed:", f"[white]{stats['completed_transfers']}[/white]")
            stats_table.add_row("❌ Failed:", f"[white]{stats['failed_transfers']}[/white]")

            total_files_overall = sum(t.get('total_files', 0) for t in self.ui_manager._torrents.values())
            completed_files_overall = sum(t.get('completed_files', 0) for t in self.ui_manager._torrents.values())
            stats_table.add_row("📂 Files:", f"[white]{completed_files_overall} / {total_files_overall}[/white]")
            stats_table.add_row("", "")

            hours, rem = divmod(elapsed, 3600)
            minutes, seconds = divmod(rem, 60)
            time_str = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
            stats_table.add_row("⏱️ Elapsed:", f"[dim]{time_str}[/dim]")

            if remaining_gb > 0 and avg_speed_hist > 0:
                eta_seconds = (remaining_gb * 1024**3) / avg_speed_hist
                eta_hours, eta_rem = divmod(eta_seconds, 3600)
                eta_minutes, _ = divmod(eta_rem, 60)
                eta_str = f"{int(eta_hours):02d}:{int(eta_minutes):02d}"
                stats_table.add_row("⏳ ETA:", f"[cyan]{eta_str}[/]")

            if recent_completions:
                recent_table = Table.grid(padding=(0, 1))
                recent_table.add_column(style="dim", no_wrap=True)
                recent_table.add_column(style="dim")
                for name, size, duration in list(recent_completions)[-3:]:
                    display_name = name[:25] + "..." if len(name) > 28 else name
                    speed = size / duration if duration > 0 else 0
                    recent_table.add_row(f"✓ {display_name}", f"{speed / (1024**2):.1f} MB/s")

                yield Group(
                    Panel(stats_table, title="[bold cyan]📊 Statistics", border_style="dim", style="on #0f3460"),
                    Panel(recent_table, title="[bold green]🎉 Recent Completions", border_style="dim", style="on #16213e")
                )
            else:
                yield Panel(stats_table, title="[bold cyan]📊 Statistics", border_style="dim", style="on #0f3460")

class _ActiveTorrentsPanel:
    """A `rich` renderable for displaying the list of active torrent transfers."""
    def __init__(self, ui_manager: "UIManagerV2"):
        """Initializes the _ActiveTorrentsPanel.

        Args:
            ui_manager: A reference to the parent `UIManagerV2` instance.
        """
        self.ui_manager = ui_manager

    def _render_progress_bar(self, percent: float, width: int = 10) -> Text:
        """Creates a custom `rich.text.Text` object to act as a progress bar.

        Args:
            percent: The completion percentage (0-100).
            width: The character width of the bar.

        Returns:
            A styled `Text` object representing the progress bar.
        """
        filled_width = int(percent / 100 * width)
        bar_char = "█"
        empty_char = "─"
        bar = bar_char * filled_width + empty_char * (width - filled_width)
        style = "green" if percent >= 100 else "blue"
        return Text.from_markup(f"[[{style}]{bar}[/]] {percent:>3.0f}%")

    def _render_file_progress_bar(self, percent: float, width: int = 15) -> Text:
        """Creates a smaller progress bar specifically for individual files.

        Args:
            percent: The completion percentage (0-100).
            width: The character width of the bar.

        Returns:
            A styled `Text` object for the file progress bar.
        """
        filled_width = int(percent / 100 * width)
        bar_char = "█"
        empty_char = "─"
        bar = bar_char * filled_width + empty_char * (width - filled_width)
        style = "green" if percent >= 100 else "blue"
        return Text.from_markup(f" [[{style}]{bar}[/]]")

    def __rich_console__(self, console: Console, options: Any) -> RenderResult:
        """Renders the active torrents list.

        This method generates a table showing each active torrent, its overall
        progress, and a detailed list of its individual files with their
        current transfer status.

        Args:
            console: The `rich` Console object.
            options: The console options.

        Yields:
            A `rich.panel.Panel` containing the active torrents table.
        """
        with self.ui_manager._lock:
            torrents = self.ui_manager._torrents
            stats = self.ui_manager._stats

            table = Table.grid(padding=(0, 1), expand=True)
            table.add_column(style="bold", no_wrap=True, width=18)
            table.add_column()

            active_count = 0
            for hash_, torrent in torrents.items():
                if torrent["status"] == "transferring":
                    active_count += 1
                    name = torrent["name"]
                    display_name = name[:40] + "..." if len(name) > 43 else name
                    progress = torrent["transferred"] / torrent["size"] * 100 if torrent["size"] > 0 else 0

                    progress_bar = self._render_progress_bar(progress)

                    file_renderables: List[Text] = []
                    files = self.ui_manager._file_status.get(hash_, {})

                    def sort_key(item):
                        status = item[1] if isinstance(item[1], str) else "queued"
                        if status in ["downloading", "uploading"]: return 0
                        if status == "failed": return 1
                        if status == "queued": return 2
                        return 3

                    sorted_files = sorted(files.items(), key=sort_key)

                    for file_path, status in sorted_files[:5]:
                        file_name = file_path.split('/')[-1]
                        file_name = file_name[:35] + "..." if len(file_name) > 38 else file_name

                        progress_bar_text = Text("")
                        file_progress_data = self.ui_manager._file_progress.get(hash_, {}).get(file_path)

                        if file_progress_data and status in ["downloading", "uploading"]:
                            transferred, total = file_progress_data
                            if total > 0:
                                file_percent = (transferred / total * 100)
                                progress_bar_text = self._render_file_progress_bar(file_percent)
                            elif transferred > 0:
                                progress_bar_text = Text.from_markup(" [[yellow]...[/]]")

                        if status == "downloading":
                            file_renderables.append(Text.from_markup(f" [blue]⇩ {file_name}[/blue]").append(progress_bar_text))
                        elif status == "uploading":
                            file_renderables.append(Text.from_markup(f" [yellow]⇧ {file_name}[/yellow]").append(progress_bar_text))
                        elif status == "failed":
                            file_renderables.append(Text.from_markup(f" [bold red]✖ {file_name}[/bold red]"))
                        elif status == "completed":
                            file_renderables.append(Text.from_markup(f" [dim]✓ {file_name}[/dim]"))
                        else:
                            file_renderables.append(Text.from_markup(f" [dim]· {file_name}[/dim]"))

                    if len(files) > 5:
                        file_renderables.append(Text.from_markup(f" [dim]... and {len(files) - 5} more.[/dim]"))

                    files_panel_content = Text("\n").join(file_renderables)
                    completed_files = torrent.get('completed_files', 0)
                    total_files = torrent.get('total_files', 0)

                    table.add_row(
                        progress_bar,
                        Group(
                            Text.from_markup(f"[bold cyan]{display_name}[/bold cyan] [dim]({completed_files}/{total_files} files)[/dim]"),
                            files_panel_content
                        )
                    )
                    table.add_row("", "")

            if active_count == 0:
                table.add_row("[dim]⏸️", "[dim]Waiting for torrents...[/dim]")

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
    """A `rich` renderable for displaying the live log footer."""
    def __init__(self, ui_manager: "UIManagerV2"):
        """Initializes the _LogPanel.

        Args:
            ui_manager: A reference to the parent `UIManagerV2` instance.
        """
        self.ui_manager = ui_manager

    def __rich_console__(self, console: Console, options: Any) -> RenderResult:
        """Renders the log panel.

        Displays the most recent log messages from the UI manager's log buffer,
        or a final status message if one has been set.
        """
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
    """A custom logging handler that redirects log records to the UIManager.

    This allows standard `logging.info()` calls from anywhere in the application
    to be displayed in the `rich` UI's live log panel.
    """
    def __init__(self, ui_manager: "UIManagerV2"):
        """Initializes the UMLoggingHandler.

        Args:
            ui_manager: The `UIManagerV2` instance to which logs will be sent.
        """
        super().__init__()
        self.ui_manager = ui_manager

    def emit(self, record: logging.LogRecord):
        """Processes a log record and sends it to the UI.

        Args:
            record: The `logging.LogRecord` to process.
        """
        if "torrent_mover.ui" in record.name:
            return
        self.ui_manager.log(record.getMessage())

class BaseUIManager(abc.ABC):
    """Abstract base class for UI managers.

    This class defines the standard interface that all UI implementations must
    adhere to, ensuring that the core application logic can interact with any
    UI in a consistent way.
    """
    def __enter__(self):
        """Enters the UI manager's context."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exits the UI manager's context, handling cleanup."""
        pass

    @abc.abstractmethod
    def set_transfer_mode(self, mode: str):
        """Sets the displayed transfer mode."""
        pass

    @abc.abstractmethod
    def set_analysis_total(self, total: int):
        """Sets the total for the torrent analysis phase."""
        pass

    @abc.abstractmethod
    def advance_analysis(self):
        """Advances the analysis progress by one step."""
        pass

    @abc.abstractmethod
    def complete_analysis(self):
        """Marks the analysis phase as complete."""
        pass

    @abc.abstractmethod
    def set_overall_total(self, total_bytes: float):
        """Sets the total size for the overall transfer progress."""
        pass

    @abc.abstractmethod
    def start_torrent_transfer(self, torrent_hash: str, torrent_name: str, total_size: float, all_files: List[str], transfer_multiplier: int = 1):
        """Initializes UI elements for a new torrent transfer."""
        pass

    @abc.abstractmethod
    def update_torrent_progress(self, torrent_hash: str, bytes_transferred: float, transfer_type: str):
        """Updates the progress of a specific torrent and the overall total."""
        pass

    @abc.abstractmethod
    def start_file_transfer(self, torrent_hash: str, file_path: str, status: str):
        """Marks an individual file as starting its transfer."""
        pass

    @abc.abstractmethod
    def complete_file_transfer(self, torrent_hash: str, file_path: str):
        """Marks an individual file as successfully transferred."""
        pass

    @abc.abstractmethod
    def fail_file_transfer(self, torrent_hash: str, file_path: str):
        """Marks an individual file as failed."""
        pass

    @abc.abstractmethod
    def complete_torrent_transfer(self, torrent_hash: str, success: bool = True):
        """Finalizes a torrent's transfer, marking it as success or failure."""
        pass

    @abc.abstractmethod
    def log(self, message: str):
        """Displays a message in the UI's logging area."""
        pass

    @abc.abstractmethod
    def set_final_status(self, message: str):
        """Sets a final, persistent message in the UI."""
        pass

    @abc.abstractmethod
    def display_stats(self, stats: Dict[str, Any]) -> None:
        """Displays a summary of statistics at the end of the run."""
        pass


class SimpleUIManager(BaseUIManager):
    """A UI manager that outputs status updates via the standard `logging` module.

    This class provides a non-interactive, logging-based UI suitable for use in
    environments like `cron` jobs, systemd services, or terminal multiplexers where
    a rich, live-updating display is not feasible.
    """
    def __init__(self):
        """Initializes the SimpleUIManager."""
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
        """Logs script exit and any errors."""
        if exc_type:
            logging.error(f"An error occurred: {exc_val}")
        logging.info("--- Torrent Mover script finished ---")

    def set_transfer_mode(self, mode: str):
        """Logs the selected transfer mode."""
        logging.info(f"Transfer Mode: {mode.upper()}")

    def log(self, message: str):
        """Logs a message, stripping any `rich` markup."""
        message = re.sub(r"\[.*?\]", "", message)
        logging.info(message)

    def set_analysis_total(self, total: int):
        """Logs the total number of torrents found."""
        self._stats["total_torrents"] = total
        logging.info(f"Analysis: Found {total} torrents to process.")

    def advance_analysis(self):
        """Does nothing in simple mode."""
        pass

    def complete_analysis(self):
        """Logs the completion of the analysis phase."""
        logging.info("Analysis: Complete.")

    def set_overall_total(self, total_bytes: float):
        """Logs the total transfer size."""
        self._stats["total_bytes"] = total_bytes
        logging.info(f"Transfer: Total size: {total_bytes / (1024**3):.2f} GB")

    def start_torrent_transfer(self, torrent_hash: str, torrent_name: str, total_size: float, all_files: List[str], transfer_multiplier: int = 1):
        """Logs the start of a new torrent transfer."""
        logging.info(f"Starting Transfer: {torrent_name}")

    def update_torrent_progress(self, torrent_hash: str, bytes_transferred: float, transfer_type: str):
        """Does nothing in simple mode to avoid log spam."""
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
        """Logs the completion or failure of a torrent transfer."""
        if success:
            self._stats["completed_transfers"] += 1
            logging.info(f"Transfer Complete: Torrent hash {torrent_hash[:10]}...")
        else:
            self._stats["failed_transfers"] += 1
            logging.error(f"Transfer Failed: Torrent hash {torrent_hash[:10]}...")

    def set_final_status(self, message: str):
        """Logs a final status message."""
        logging.info(f"Status: {message}")

    def display_stats(self, stats: Dict[str, Any]) -> None:
        """Logs a final summary of statistics."""
        logging.info("--- Final Statistics ---")
        logging.info(f"Total Bytes Transferred: {self._stats['transferred_bytes'] / (1024**3):.2f} GB")
        logging.info(f"Successful Transfers: {self._stats['completed_transfers']}")
        logging.info(f"Failed Transfers: {self._stats['failed_transfers']}")
        logging.info(f"Total Duration: {time.time() - self._stats['start_time']:.2f} seconds")


class UIManagerV2(BaseUIManager):
    """An advanced, real-time terminal UI using the `rich` library.

    This class orchestrates a complex layout of panels, progress bars, and tables
    to provide a detailed, live view of the application's state. It uses custom
    renderable classes and a background thread to keep the display updated
    without blocking the main application logic.
    """

    def __init__(self, version: str = "", rich_handler: Optional[logging.Handler] = None):
        """Initializes the UIManagerV2.

        Args:
            version: The application version string to display in the header.
            rich_handler: A reference to the `RichHandler` to be temporarily
                        removed and replaced by the UI's custom handler.
        """
        self.console = Console()
        self._lock = threading.RLock()
        self._live: Optional[Live] = None
        self.version = version
        self._rich_handler_ref = rich_handler
        self._um_log_handler = UMLoggingHandler(self)
        self.transfer_mode = ""
        self._log_buffer: Deque[Text] = deque(maxlen=20)
        self._current_header_string_template: str = ""
        self._last_header_text_part: str = ""
        self._current_status: Optional[str] = None
        self._analysis_complete = False

        self._torrents: OrderedDict[str, Dict] = OrderedDict()
        self._active_torrents: Deque[str] = deque(maxlen=5)
        self._completed_hashes: set = set()
        self._recent_completions: Deque[tuple] = deque(maxlen=5)

        self._file_lists: Dict[str, List[str]] = {}
        self._file_status: Dict[str, Dict[str, str]] = {}
        self._file_progress: Dict[str, Dict[str, Tuple[int, int]]] = {}


        self._stats = {
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
        self._dl_speed_history = deque(maxlen=60)
        self._ul_speed_history = deque(maxlen=60)


        self.layout = Layout()
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=7)
        )

        self.layout["body"].split_row(
            Layout(name="left", ratio=3),
            Layout(name="right", ratio=1)
        )

        self._setup_header()
        self._setup_progress()
        self._setup_stats_panel()
        self._setup_footer()

    def _setup_header(self):
        """Initializes the header panel of the UI layout."""
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
        """Initializes the progress bars and the active torrents panel."""
        self.main_progress = Progress(
            TextColumn("[bold]{task.description}", justify="left"),
            BarColumn(bar_width=None, complete_style="green", finished_style="bold green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            "•",
            DownloadColumn(binary_units=True),
            "•",
            _SpeedColumn("dl_speed", "DL", "green"),
            "•",
            _SpeedColumn("current_ul_speed", "UL", "yellow", ui_manager=self),
            "•",
            TimeRemainingColumn(),
            expand=True,
        )

        self.analysis_task = self.main_progress.add_task(
            "[cyan]📊 Analysis", total=100, visible=False
        )

        self.overall_task = self.main_progress.add_task(
            "[green]📦 Overall Progress",
            total=100,
            visible=False,
            fields={'dl_speed': '0.00 MB/s', 'ul_speed': '0.00 MB/s'}
        )

        left_group = Group(
            Panel(
                self.main_progress,
                title="[bold green]📈 Transfer Progress",
                border_style="dim",
                style="on #16213e"
            ),
            _ActiveTorrentsPanel(self)
        )
        self.layout["left"].update(left_group)

    def _setup_stats_panel(self):
        """Initializes the statistics panel."""
        self.layout["right"].update(_StatsPanel(self))

    def _setup_footer(self):
        """Initializes the log panel footer."""
        self.layout["footer"].update(_LogPanel(self))

    def __enter__(self):
        """Starts the `rich.Live` display and the stats updater thread."""
        root_logger = logging.getLogger()
        if self._rich_handler_ref:
            root_logger.removeHandler(self._rich_handler_ref)
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
        """Stops the live display and cleans up resources."""
        if self._live:
            self._stats_thread_stop.set()
            if self._stats_thread.is_alive():
                self._stats_thread.join(timeout=2.0)
            self.main_progress.stop()
            self._live.stop()
        root_logger = logging.getLogger()
        root_logger.removeHandler(self._um_log_handler)
        if self._rich_handler_ref:
            root_logger.addHandler(self._rich_handler_ref)

    def set_final_status(self, message: str):
        """Sets a final, persistent status message in the log panel."""
        with self._lock:
            self._current_status = message

    def _stats_updater(self):
        """Background thread that periodically calculates and updates UI stats."""
        while not self._stats_thread_stop.wait(1.0):
            dl_speed_str = "0.00 MB/s"
            ul_speed_str = "0.00 MB/s"
            task_exists = False

            with self._lock:
                current_dl_speed = 0.0
                current_ul_speed = 0.0
                time_since_last_dl = time.time() - self._stats["last_dl_speed_check"]
                time_since_last_ul = time.time() - self._stats["last_ul_speed_check"]

                if time_since_last_dl >= 1.0:
                    bytes_since_last = self._stats["transferred_dl_bytes"] - self._stats["last_dl_bytes"]
                    current_dl_speed = bytes_since_last / time_since_last_dl
                    self._stats["last_dl_bytes"] = self._stats["transferred_dl_bytes"]
                    self._stats["last_dl_speed_check"] = time.time()
                    self._stats["current_dl_speed"] = current_dl_speed
                    self._dl_speed_history.append(current_dl_speed)
                else:
                    current_dl_speed = self._stats.get("current_dl_speed", 0.0)

                if time_since_last_ul >= 1.0:
                    bytes_since_last = self._stats["transferred_ul_bytes"] - self._stats["last_ul_bytes"]
                    current_ul_speed = bytes_since_last / time_since_last_ul
                    self._stats["last_ul_bytes"] = self._stats["transferred_ul_bytes"]
                    self._stats["last_ul_speed_check"] = time.time()
                    self._stats["current_ul_speed"] = current_ul_speed
                    self._ul_speed_history.append(current_ul_speed)
                else:
                    current_ul_speed = self._stats.get("current_ul_speed", 0.0)

                current_total_speed = current_dl_speed + current_ul_speed
                if current_total_speed > self._stats["peak_speed"]:
                    self._stats["peak_speed"] = current_total_speed

                dl_speed_str = f"{current_dl_speed / (1024**2):.2f} MB/s"
                ul_speed_str = f"{current_ul_speed / (1024**2):.2f} MB/s"

                try:
                    task_exists = hasattr(self.main_progress, 'tasks') and self.overall_task in [t.id for t in self.main_progress.tasks]
                except Exception:
                    task_exists = False

            if task_exists:
                try:
                    self.main_progress.update(
                        self.overall_task,
                        fields={
                            'dl_speed': dl_speed_str,
                            'ul_speed': ul_speed_str
                        }
                    )
                except Exception as e:
                    logging.debug(f"Could not update overall task speeds (non-locked): {e}")

    def set_transfer_mode(self, mode: str):
        """Sets the displayed transfer mode."""
        with self._lock:
            self.transfer_mode = mode
            self._setup_header()

    def set_analysis_total(self, total: int):
        """Sets the total for the torrent analysis progress bar."""
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
        """Sets the total size for the overall transfer progress bar."""
        with self._lock:
            self._stats["total_bytes"] = total_bytes
            self.main_progress.update(self.overall_task, total=total_bytes, visible=True)

    def start_torrent_transfer(self, torrent_hash: str, torrent_name: str, total_size: float, all_files: List[str], transfer_multiplier: int = 1):
        """Initializes the UI state for a new torrent transfer."""
        with self._lock:
            total_files_count = len(all_files)
            self._torrents[torrent_hash] = {
                "name": torrent_name,
                "size": total_size * transfer_multiplier,
                "total_files": total_files_count,
                "completed_files": 0,
                "transferred": 0,
                "status": "transferring",
                "start_time": time.time()
            }
            self._file_lists[torrent_hash] = all_files
            self._file_status[torrent_hash] = {file_name: "queued" for file_name in all_files}
            self._active_torrents.append(torrent_hash)
            self._stats["active_transfers"] += 1

    def update_torrent_progress(self, torrent_hash: str, bytes_transferred: float, transfer_type: str):
        """Updates progress for a torrent and the overall total.

        Args:
            torrent_hash: The hash of the torrent being updated.
            bytes_transferred: The number of new bytes transferred.
            transfer_type: Either 'download' or 'upload'.
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
        """Updates the status of an individual file to show it is transferring.

        Args:
            torrent_hash: The hash of the parent torrent.
            file_path: The path of the file.
            status: The new status (e.g., 'downloading', 'uploading').
        """
        with self._lock:
            if torrent_hash in self._file_status and file_path in self._file_status[torrent_hash]:
                self._file_status[torrent_hash][file_path] = status

    def complete_file_transfer(self, torrent_hash: str, file_path: str):
        """Marks an individual file's transfer as complete."""
        with self._lock:
            if torrent_hash in self._file_status and file_path in self._file_status[torrent_hash]:
                if self._file_status[torrent_hash][file_path] != "completed":
                    self._file_status[torrent_hash][file_path] = "completed"
                    if torrent_hash in self._torrents:
                        current_completed = self._torrents[torrent_hash].get("completed_files", 0)
                        total_files = self._torrents[torrent_hash].get("total_files", 0)
                        if current_completed < total_files:
                            self._torrents[torrent_hash]["completed_files"] = current_completed + 1

    def fail_file_transfer(self, torrent_hash: str, file_path: str):
        """Marks an individual file's transfer as failed."""
        with self._lock:
            if torrent_hash in self._file_status and file_path in self._file_status[torrent_hash]:
                self._file_status[torrent_hash][file_path] = "failed"

    def complete_torrent_transfer(self, torrent_hash: str, success: bool = True):
        """Finalizes a torrent's transfer status."""
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
        """Adds a message to the live log panel."""
        with self._lock:
            timestamp = time.strftime("%H:%M:%S")
            self._log_buffer.append(Text.from_markup(f"[{timestamp}] {message}"))

    def display_stats(self, stats: Dict[str, Any]) -> None:
        """Displays a final summary of statistics to the console."""
        self.console.print("--- Final Statistics ---", style="bold green")
        self.console.print(f"Total Bytes Transferred: {self._stats['transferred_bytes'] / (1024**3):.2f} GB")
        self.console.print(f"Peak Speed: {self._stats['peak_speed'] / (1024**2):.2f} MB/s")
        self.console.print(f"Successful Transfers: {self._stats['completed_transfers']}")
        self.console.print(f"Failed Transfers: {self._stats['failed_transfers']}")
        self.console.print(f"Total Duration: {time.time() - self._stats['start_time']:.2f} seconds")

__all__ = ["BaseUIManager", "SimpleUIManager", "UIManagerV2"]
