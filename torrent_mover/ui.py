# torrent_mover/ui.py
import logging
from rich.logging import RichHandler
from rich.containers import Renderables
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    Progress,
    TextColumn,
    BarColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
    TotalFileSizeColumn,
    MofNCompleteColumn,
    TaskID,
    FileSizeColumn,
    DownloadColumn,
)
from rich.table import Table, Column
from rich.text import Text
import threading
from collections import OrderedDict, deque
from typing import Dict, Any, List, Optional, OrderedDict as OrderedDictType, Deque, Tuple
from rich.layout import Layout
import time


class RichLogHandler(logging.Handler):
    """A logging handler that captures the last N records for display in Rich."""
    def __init__(self, max_lines: int = 15):
        super().__init__()
        self.max_lines = max_lines
        self.records: Deque[logging.LogRecord] = deque(maxlen=max_lines)
        self.setFormatter(logging.Formatter('%(message)s'))

    def emit(self, record: logging.LogRecord):
        self.records.append(record)

    def get_renderables(self) -> Renderables:
        """Format the captured records into Rich Text objects."""
        lines = []
        for record in self.records:
            if record.levelno >= logging.ERROR:
                style = "bold red"
            elif record.levelno >= logging.WARNING:
                style = "yellow"
            elif record.levelno >= logging.INFO:
                style = "none"
            else: # DEBUG
                style = "dim"
            lines.append(Text(self.format(record), style=style))
        return Renderables(lines)


class UIManagerV2:
    def __init__(self, version: str = ""):
        self.console = Console()
        self._lock = threading.RLock()
        self._live: Optional[Live] = None

        # --- CORRECTED ORDER: Define all attributes first ---
        # Data structures
        self._torrents: OrderedDict[str, Dict] = OrderedDict()
        self._active_torrents: Deque[str] = deque(maxlen=5)  # Last 5 active
        self._completed_hashes: set = set()
        self._completed_torrents_log: Deque[Tuple[str, bool]] = deque(maxlen=10) # Store (name, success)

        # Logging
        self.log_handler = RichLogHandler(max_lines=15) # Keep last 15 log lines
        # Add handler ONLY to the root logger to capture everything
        logging.getLogger().addHandler(self.log_handler)
        self.log_renderables = Renderables([]) # Initialize log renderables

        # Statistics (live updated)
        self._stats = {
            "total_bytes": 0,
            "transferred_bytes": 0,
            "start_time": time.time(),
            "active_transfers": 0,
            "completed_transfers": 0,
            "failed_transfers": 0,
            "total_torrents": 0, # Added for clarity
        }
        self._stats_thread: Optional[threading.Thread] = None
        self._stats_thread_stop = threading.Event()


        # Completed table (initialize here)
        self.completed_table = Table.grid(padding=(0, 1))
        self.completed_table.add_column("Status", width=4)
        self.completed_table.add_column("Torrent Name")
        # --- END ATTRIBUTE DEFINITIONS ---


        # Create layout
        self.layout = Layout()
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=3)
        )

        # Split body into left (torrents & logs) and right (stats & completed)
        self.layout["body"].split_row(
            Layout(name="left_panel", ratio=2),
            Layout(name="right_panel", ratio=1)
        )

        # Split left panel vertically
        self.layout["left_panel"].split(
             Layout(name="torrents", ratio=3), # Give more space to progress bars
             Layout(name="logs", ratio=1)      # Add logs panel below
        )

        # Split right panel vertically
        self.layout["right_panel"].split(
             Layout(name="stats", ratio=1),
             Layout(name="completed", ratio=1) # Add completed panel below
        )

        # Initialize components (Now safe to call setup methods)
        self._setup_header(version)
        self._setup_progress()
        self._setup_stats_panel()
        self._setup_log_panel()
        self._setup_completed_panel()
        self._setup_footer()

    def _setup_header(self, version: str):
        self.header_text = Text(f"Torrent Mover v{version} - Initializing...", justify="center", style="bold magenta")
        self.layout["header"].update(Panel(self.header_text, border_style="magenta"))

    def _setup_progress(self):
        self.main_progress = Progress(
            TextColumn("[bold]{task.description}"), BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), "•",
            DownloadColumn(), "•", TransferSpeedColumn(), "•", TimeRemainingColumn(), expand=True
        )
        self.analysis_task = self.main_progress.add_task("[cyan]Analysis", total=100, visible=False)
        self.overall_task = self.main_progress.add_task("[green]Overall Transfer", total=100, visible=False)
        self.current_torrent_task = self.main_progress.add_task("[yellow]Current: (none)", total=100, visible=False)
        self.files_progress = Progress(
            TextColumn("[dim]{task.description}"), BarColumn(bar_width=30),
            TextColumn("{task.percentage:>3.0f}%"), expand=True
        )
        self.active_file_tasks: Dict[str, TaskID] = {}
        progress_group = Group(
            Panel(self.main_progress, title="[bold]Transfer Progress", border_style="green"),
            Panel(self.files_progress, title="[bold]Active Files (Last 5)", border_style="blue", height=8)
        )
        self.layout["torrents"].update(progress_group)

    def _setup_stats_panel(self):
        self.stats_table = Table.grid(padding=(0, 2))
        self.stats_table.add_column(style="dim", justify="right")
        self.stats_table.add_column()
        self._update_stats_display()
        self.layout["stats"].update(Panel(self.stats_table, title="[bold]Live Stats", border_style="cyan"))

    def _setup_log_panel(self):
        self.log_renderables = self.log_handler.get_renderables()
        self.layout["logs"].update(Panel(self.log_renderables, title="[bold]Logs (Last 15)", border_style="yellow"))

    def _setup_completed_panel(self):
        self.completed_table = Table.grid(padding=(0, 1))
        self.completed_table.add_column("Status")
        self.completed_table.add_column("Torrent Name")
        self._update_completed_display()
        self.layout["completed"].update(Panel(self.completed_table, title="[bold]Recently Completed (Last 10)", border_style="dim"))

    def _setup_footer(self):
        self.footer_text = Text("Ready", justify="center", style="dim")
        self.layout["footer"].update(Panel(self.footer_text, border_style="dim"))

    def _update_completed_display(self):
        with self._lock:
            self.completed_table = Table.grid(padding=(0, 1))
            self.completed_table.add_column("Status", width=4)
            self.completed_table.add_column("Torrent Name")
            for name, success in reversed(self.completed_torrents_log):
                status_icon = "[green]✓[/]" if success else "[bold red]✗[/]"
                self.completed_table.add_row(status_icon, Text(name[:50], overflow="ellipsis"))
            renderable = self.completed_table if self._completed_torrents_log else Text(" ", justify="center")
            if isinstance(self.layout["completed"].renderable, Panel):
                self.layout["completed"].renderable.renderable = renderable
            else:
                self.layout["completed"].update(Panel(renderable, title="[bold]Recently Completed (Last 10)", border_style="dim"))

    def _update_stats_display(self):
        with self._lock:
            elapsed = time.time() - self._stats["start_time"]
            speed = self._stats["transferred_bytes"] / elapsed if elapsed > 1 else 0
            self.stats_table = Table.grid(padding=(0, 2))
            self.stats_table.add_column(style="dim", justify="right")
            self.stats_table.add_column()
            self.stats_table.add_row("Transferred:", f"{self._stats['transferred_bytes'] / (1024**3):.2f} GB")
            self.stats_table.add_row("Speed:", f"{speed / (1024**2):.2f} MB/s")
            self.stats_table.add_row("Active:", f"{self._stats['active_transfers']}")
            self.stats_table.add_row("Completed:", f"[green]{self._stats['completed_transfers']}[/]")
            self.stats_table.add_row("Failed:", f"[red]{self._stats['failed_transfers']}[/]")
            self.stats_table.add_row("Elapsed:", f"{int(elapsed)} sec")
            if self._stats["transferred_bytes"] > 0 and self._stats["total_bytes"] > 0:
                remaining = self._stats["total_bytes"] - self._stats["transferred_bytes"]
                if remaining > 0 and speed > 0:
                    self.stats_table.add_row("ETA:", f"{int(remaining / speed)} sec")
            queued = self._stats["total_torrents"] - self._stats["completed_transfers"] - self._stats["failed_transfers"] - self._stats["active_transfers"]
            self.stats_table.add_row("In Queue:", str(queued))
            self.layout["stats"].update(Panel(self.stats_table, title="[bold]Live Stats", border_style="cyan"))

    def __enter__(self):
        self._live = Live(self.layout, console=self.console, screen=True, redirect_stderr=False, refresh_per_second=4)
        self._live.start()
        self._stats_thread_stop = threading.Event()
        self._stats_thread = threading.Thread(target=self._stats_updater, daemon=True)
        self._stats_thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live:
            self._stats_thread_stop.set()
            if hasattr(self, '_stats_thread'):
                self._stats_thread.join()
            self.main_progress.stop()
            self.files_progress.stop()
            self._live.stop()
            logging.getLogger().removeHandler(self.log_handler)

    def _stats_updater(self):
        while not self._stats_thread_stop.wait(1.0):
            self._update_stats_display()

    def set_analysis_total(self, total: int):
        with self._lock:
            self._stats["total_torrents"] = total
        self.main_progress.update(self.analysis_task, total=total, visible=True)

    def advance_analysis(self):
        self.main_progress.update(self.analysis_task, advance=1)

    def set_overall_total(self, total_bytes: float):
        with self._lock:
            self._stats["total_bytes"] = total_bytes
        self.main_progress.update(self.overall_task, total=total_bytes, visible=True)

    def start_torrent_transfer(self, torrent_hash: str, torrent_name: str, total_size: float, transfer_multiplier: int = 1):
        with self._lock:
            self._torrents[torrent_hash] = {"name": torrent_name, "size": total_size * transfer_multiplier, "transferred": 0, "status": "transferring"}
            self._active_torrents.append(torrent_hash)
            self._stats["active_transfers"] += 1
        self.main_progress.update(self.current_torrent_task, description=f"[yellow]Current: {torrent_name[:40]}...", completed=0, total=total_size * transfer_multiplier, visible=True)

    def update_torrent_progress(self, torrent_hash: str, bytes_transferred: float):
        with self._lock:
            if torrent_hash in self._torrents:
                self._torrents[torrent_hash]["transferred"] += bytes_transferred
            self._stats["transferred_bytes"] += bytes_transferred
        self.main_progress.update(self.overall_task, advance=bytes_transferred)
        if self.main_progress.tasks[self.current_torrent_task].description.startswith(f"[yellow]Current: {self._torrents.get(torrent_hash, {}).get('name', '')[:40]}"):
            self.main_progress.update(self.current_torrent_task, advance=bytes_transferred)

    def start_file_transfer(self, torrent_hash: str, file_path: str, file_size: int):
        if len(self.active_file_tasks) >= 5:
            try:
                oldest_file = next(iter(self.active_file_tasks))
                task_id = self.active_file_tasks.pop(oldest_file)
                self.files_progress.remove_task(task_id)
            except StopIteration:
                pass
        file_name = file_path.split('/')[-1]
        task_id = self.files_progress.add_task(file_name[:50], total=file_size)
        self.active_file_tasks[file_path] = task_id

    def update_file_progress(self, file_path: str, bytes_transferred: int):
        if file_path in self.active_file_tasks:
            self.files_progress.update(self.active_file_tasks[file_path], advance=bytes_transferred)

    def complete_file_transfer(self, file_path: str):
        if file_path in self.active_file_tasks:
            task_id = self.active_file_tasks.pop(file_path)
            self.files_progress.remove_task(task_id)

    def complete_torrent_transfer(self, torrent_hash: str, success: bool = True):
        with self._lock:
            if torrent_hash in self._torrents:
                self._torrents[torrent_hash]["status"] = "completed" if success else "failed"
                self._completed_hashes.add(torrent_hash)
                self._stats["active_transfers"] = max(0, self._stats["active_transfers"] - 1)
                if success:
                    self._stats["completed_transfers"] += 1
                else:
                    self._stats["failed_transfers"] += 1
        self.main_progress.update(self.current_torrent_task, visible=False, description="[yellow]Current: (none)")

    def update_header(self, text: str):
        self.header_text = Text(text, justify="center", style="bold magenta")
        self.layout["header"].update(Panel(self.header_text, border_style="magenta"))

    def update_footer(self, text: str):
        self.footer_text = Text(text, justify="center", style="dim")
        self.layout["footer"].update(Panel(self.footer_text, border_style="dim"))

    def log(self, message: str):
        self.console.log(message)

    def display_stats(self, stats: Dict[str, Any]) -> None:
        self.console.log("--- Final Statistics ---")
        self.console.log(f"Total Bytes Transferred: {self._stats['transferred_bytes'] / (1024**3):.2f} GB")
        self.console.log(f"Successful Transfers: {self._stats['completed_transfers']}")
        self.console.log(f"Failed Transfers: {self._stats['failed_transfers']}")
        self.console.log(f"Total Duration: {time.time() - self._stats['start_time']:.2f} seconds")
