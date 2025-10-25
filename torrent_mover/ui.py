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
from typing import Dict, Any, List, Optional, OrderedDict as OrderedDictType, Deque
from rich.layout import Layout
import time


class RichLogHandler(logging.Handler):
    """A logging handler that captures the last N records for display in Rich."""
    def __init__(self, max_lines: int = 15):
        super().__init__()
        self.max_lines = max_lines
        self.records: Deque[logging.LogRecord] = deque(maxlen=max_lines)
        # Use Rich's formatter for nice output
        self.setFormatter(logging.Formatter('%(message)s'))


    def emit(self, record: logging.LogRecord):
        # We store the raw record and format it later
        self.records.append(record)

    def get_renderables(self) -> Renderables:
        """Format the captured records into Rich Text objects."""
        lines = []
        for record in self.records:
            # Basic level styling
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
    """
    Redesigned UI with focus on performance and information density.

    KEY IMPROVEMENTS:
    - Single Progress object shared across all torrents (not per-torrent)
    - Smart file display: only show last 5 active files
    - Live stats panel
    - Persistent layout (no rebuilds)
    - Ring buffer for torrent history
    """

    def __init__(self, version: str = ""):
        self.console = Console()
        self._lock = threading.RLock()
        self._live: Optional[Live] = None

        # Data structures
        self._torrents: OrderedDict[str, Dict] = OrderedDict()
        self._active_torrents: Deque[str] = deque(maxlen=5)  # Last 5 active
        self._completed_hashes: set = set()
        self._completed_torrents_log: Deque[Tuple[str, bool]] = deque(maxlen=10)

        # Logging
        self.log_handler = RichLogHandler(max_lines=15)
        logging.getLogger().addHandler(self.log_handler)

        # Statistics (live updated)
        self._stats = {
            "total_torrents": 0,
            "total_bytes": 0,
            "transferred_bytes": 0,
            "start_time": time.time(),
            "active_transfers": 0,
            "completed_transfers": 0,
            "failed_transfers": 0
        }

        # --- LAYOUT DEFINITION ---
        self.layout = Layout()
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=3)
        )
        self.layout["body"].split_row(
            Layout(name="left_panel", ratio=2),
            Layout(name="right_panel", ratio=1)
        )
        self.layout["left_panel"].split(
             Layout(name="torrents", ratio=3),
             Layout(name="logs", ratio=1)
        )
        self.layout["right_panel"].split(
             Layout(name="stats", ratio=1),
             Layout(name="completed", ratio=1)
        )
        # --- END LAYOUT DEFINITION ---

        # Initialize components
        self._setup_header(version)
        self._setup_progress()
        self._setup_stats_panel()
        self._setup_log_panel()
        self._setup_completed_panel()
        self._setup_footer()

    def _setup_header(self, version: str):
        """Setup header with title and overall progress."""
        self.header_text = Text(f"Torrent Mover v{version} - Initializing...", justify="center", style="bold magenta")
        self.layout["header"].update(
            Panel(self.header_text, border_style="magenta")
        )

    def _setup_progress(self):
        """Setup shared progress bars."""
        # Main progress for all transfers
        self.main_progress = Progress(
            TextColumn("[bold]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            "•",
            DownloadColumn(),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            expand=True
        )

        # Analysis progress
        self.analysis_task = self.main_progress.add_task(
            "[cyan]Analysis", total=100, visible=False
        )

        # Overall transfer progress
        self.overall_task = self.main_progress.add_task(
            "[green]Overall Transfer", total=100, visible=False
        )

        # Current torrent progress (updates as we switch torrents)
        self.current_torrent_task = self.main_progress.add_task(
            "[yellow]Current: (none)", total=100, visible=False
        )

        # Active files progress (shows last 5 files)
        self.files_progress = Progress(
            TextColumn("[dim]{task.description}"),
            BarColumn(bar_width=30),
            TextColumn("{task.percentage:>3.0f}%"),
            expand=True
        )
        self.active_file_tasks: Dict[str, TaskID] = {}

        # Combine into torrents panel
        progress_group = Group(
            Panel(self.main_progress, title="[bold]Transfer Progress", border_style="green"),
            Panel(self.files_progress, title="[bold]Active Files (Last 5)", border_style="blue", height=8)
        )

        self.layout["torrents"].update(progress_group)

    def _setup_stats_panel(self):
        """Setup live statistics panel."""
        self.stats_table = Table.grid(padding=(0, 2))
        self.stats_table.add_column(style="dim", justify="right")
        self.stats_table.add_column()

        self._update_stats_display()

        self.layout["stats"].update(
            Panel(self.stats_table, title="[bold]Live Stats", border_style="cyan")
        )

    def _setup_log_panel(self):
        """Setup the live log panel."""
        self.log_renderables = self.log_handler.get_renderables()
        self.layout["logs"].update(
            Panel(self.log_renderables, title="[bold]Logs (Last 15)", border_style="yellow")
        )

    def _setup_completed_panel(self):
        """Setup the completed torrents panel."""
        self.completed_table = Table.grid(padding=(0, 1))
        self.completed_table.add_column("Status")
        self.completed_table.add_column("Torrent Name")
        self._update_completed_display() # Initial empty setup
        self.layout["completed"].update(
            Panel(self.completed_table, title="[bold]Recently Completed (Last 10)", border_style="dim")
        )

    def _setup_footer(self):
        """Setup footer with queue status."""
        self.footer_text = Text("Ready", justify="center", style="dim")
        self.layout["footer"].update(
            Panel(self.footer_text, border_style="dim")
        )

    def _update_completed_display(self):
        """Update the completed torrents table."""
        with self._lock:
            self.completed_table = Table.grid(padding=(0, 1))
            self.completed_table.add_column("Status", width=4) # Icon column
            self.completed_table.add_column("Torrent Name")
            for name, success in reversed(self.completed_torrents_log): # Show newest first
                status_icon = "[green]✓[/]" if success else "[bold red]✗[/]"
                self.completed_table.add_row(status_icon, Text(name[:50], overflow="ellipsis")) # Truncate long names

            # Update panel content (only renderable, not title/border)
            renderable = self.completed_table if self._completed_torrents_log else Text(" ", justify="center") # Show blank if empty
            if isinstance(self.layout["completed"].renderable, Panel):
                 self.layout["completed"].renderable.renderable = renderable
            else: # First time setup
                 self.layout["completed"].update(
                     Panel(renderable, title="[bold]Recently Completed (Last 10)", border_style="dim")
                 )

    def _update_log_display(self):
        """Update the log display panel."""
        self.log_renderables = self.log_handler.get_renderables()
        # Update panel content (only renderable, not title/border)
        if isinstance(self.layout["logs"].renderable, Panel):
             self.layout["logs"].renderable.renderable = self.log_renderables
        else: # First time setup (shouldn't happen often)
             self.layout["logs"].update(
                 Panel(self.log_renderables, title="[bold]Logs (Last 15)", border_style="yellow")
             )

    def _update_stats_display(self):
        """Update the stats table (called periodically)."""
        with self._lock:
            elapsed = time.time() - self._stats["start_time"]
            speed = self._stats["transferred_bytes"] / elapsed if elapsed > 1 else 0

            # Clear and rebuild table
            self.stats_table = Table.grid(padding=(0, 2))
            self.stats_table.add_column(style="dim", justify="right")
            self.stats_table.add_column()

            self.stats_table.add_row("Transferred:", f"{self._stats['transferred_bytes'] / (1024**3):.2f} GB")
            self.stats_table.add_row("Speed:", f"{speed / (1024**2):.2f} MB/s")
            self.stats_table.add_row("Active:", f"{self._stats['active_transfers']}")
            self.stats_table.add_row("Completed:", f"[green]{self._stats['completed_transfers']}[/]")
            self.stats_table.add_row("Failed:", f"[red]{self._stats['failed_transfers']}[/]")
            self.stats_table.add_row("Elapsed:", f"{int(elapsed)} sec")

            # ETA calculation
            if self._stats["transferred_bytes"] > 0 and self._stats["total_bytes"] > 0:
                remaining = self._stats["total_bytes"] - self._stats["transferred_bytes"]
                if remaining > 0 and speed > 0:
                    eta = remaining / speed
                    self.stats_table.add_row("ETA:", f"{int(eta)} sec")

            # Queue status
            queued = self._stats["total_torrents"] - self._stats["completed_transfers"] - self._stats["failed_transfers"] - self._stats["active_transfers"]
            self.stats_table.add_row("In Queue:", str(queued))

            # Update panel
            self.layout["stats"].update(
                Panel(self.stats_table, title="[bold]Live Stats", border_style="cyan")
            )

    def __enter__(self):
        self._live = Live(
            self.layout,
            console=self.console,
            screen=True,
            redirect_stderr=False,
            refresh_per_second=4 # Reduced from 10 (less CPU)
        )
        self._live.start()

        # Start stats update thread
        self._stats_thread_stop = threading.Event()
        self._stats_thread = threading.Thread(target=self._stats_updater, daemon=True)
        self._stats_thread.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live:
            self._stats_thread_stop.set()
            if hasattr(self, '_stats_thread'): # Check if thread started
                 self._stats_thread.join()
            self.main_progress.stop()
            self.files_progress.stop()
            self._live.stop()
            # --- REMOVE HANDLER ---
            logging.getLogger().removeHandler(self.log_handler)
            # --- END REMOVE ---

    def _stats_updater(self):
        """Background thread to update stats, logs, and completed list periodically."""
        while not self._stats_thread_stop.wait(1.0): # Update every second
            self._update_stats_display()
            self._update_log_display() # Add this
            self._update_completed_display() # Add this

    # ===== Public API =====

    def set_analysis_total(self, total: int):
        """Set total number of torrents to analyze."""
        with self._lock:
            self._stats["total_torrents"] = total
        self.main_progress.update(self.analysis_task, total=total, visible=True)

    def advance_analysis(self):
        """Mark one torrent as analyzed."""
        self.main_progress.update(self.analysis_task, advance=1)

    def set_overall_total(self, total_bytes: float):
        """Set total bytes to transfer."""
        with self._lock:
            self._stats["total_bytes"] = total_bytes
        self.main_progress.update(self.overall_task, total=total_bytes, visible=True)

    def start_torrent_transfer(self, torrent_hash: str, torrent_name: str,
                               total_size: float, transfer_multiplier: int = 1):
        """Begin transferring a torrent."""
        with self._lock:
            self._torrents[torrent_hash] = {
                "name": torrent_name,
                "size": total_size * transfer_multiplier,
                "transferred": 0,
                "status": "transferring"
            }
            self._active_torrents.append(torrent_hash)
            self._stats["active_transfers"] += 1

        # Update current torrent progress bar
        self.main_progress.update(
            self.current_torrent_task,
            description=f"[yellow]Current: {torrent_name[:40]}...",
            completed=0,
            total=total_size * transfer_multiplier,
            visible=True
        )

    def update_torrent_progress(self, torrent_hash: str, bytes_transferred: float):
        """Update progress for a torrent."""
        with self._lock:
            if torrent_hash in self._torrents:
                self._torrents[torrent_hash]["transferred"] += bytes_transferred
            self._stats["transferred_bytes"] += bytes_transferred

        # Update progress bars
        self.main_progress.update(self.overall_task, advance=bytes_transferred)
        # Only update current_torrent_task if it's the one we're tracking
        if self.main_progress.tasks[self.current_torrent_task].description.endswith(f"{torrent_hash[:10]}..."):
             self.main_progress.update(self.current_torrent_task, advance=bytes_transferred)
        elif self.main_progress.tasks[self.current_torrent_task].description.startswith(f"[yellow]Current: {self._torrents[torrent_hash]['name'][:40]}"):
            self.main_progress.update(self.current_torrent_task, advance=bytes_transferred)


    def start_file_transfer(self, torrent_hash: str, file_path: str, file_size: int):
        """Start tracking a file transfer."""
        with self._lock:
            torrent_name = self._torrents.get(torrent_hash, {}).get("name", "Unknown Torrent")
            short_torrent_name = torrent_name[:25] # Truncate torrent name

            # Only show last 5 files globally
            if len(self.active_file_tasks) >= 5:
                # Remove oldest
                try:
                    oldest_file_key = next(iter(self.active_file_tasks)) # Get key (full path)
                    task_id = self.active_file_tasks.pop(oldest_file_key)
                    self.files_progress.remove_task(task_id)
                except StopIteration:
                    pass # No tasks to remove

            # Add new file
            file_name_only = file_path.split('/')[-1]
            # --- MODIFIED DESCRIPTION ---
            display_name = f"[dim]{short_torrent_name}:[/] {file_name_only}"
            task_id = self.files_progress.add_task(
                display_name[:60], # Limit total length
                total=file_size,
                start=False # Don't start automatically, let update start it
            )
            self.active_file_tasks[file_path] = task_id # Use full path as key

    def update_file_progress(self, file_path: str, bytes_transferred: int):
        """Update progress for a specific file."""
        if file_path in self.active_file_tasks:
            task_id = self.active_file_tasks[file_path]
            task = self.files_progress._tasks[task_id] # Access task directly
            if not task.started:
                self.files_progress.start_task(task_id)
            self.files_progress.update(
                task_id,
                advance=bytes_transferred
            )

    def complete_file_transfer(self, file_path: str):
        """Mark a file as completed."""
        if file_path in self.active_file_tasks:
            task_id = self.active_file_tasks.pop(file_path)
            self.files_progress.remove_task(task_id)

    def complete_torrent_transfer(self, torrent_hash: str, success: bool = True):
        """Mark a torrent as completed."""
        torrent_name = "Unknown Torrent"
        with self._lock:
            if torrent_hash in self._torrents:
                torrent_name = self._torrents[torrent_hash]["name"] # Get name before potentially removing
                self._torrents[torrent_hash]["status"] = "completed" if success else "failed"
                self._completed_hashes.add(torrent_hash)
                self._stats["active_transfers"] = max(0, self._stats["active_transfers"] - 1)

                if success:
                    self._stats["completed_transfers"] += 1
                else:
                    self._stats["failed_transfers"] += 1

                # --- ADD TO COMPLETED LOG ---
                self._completed_torrents_log.append((torrent_name, success))
                # --- END ADD ---


        # Hide current torrent progress
        self.main_progress.update(self.current_torrent_task, visible=False, description="[yellow]Current: (none)")
        # No need to manually call _update_completed_display here, the background thread handles it

    def update_header(self, text: str):
        """Update header text."""
        self.header_text = Text(text, justify="center", style="bold magenta")
        self.layout["header"].update(Panel(self.header_text, border_style="magenta"))

    def update_footer(self, text: str):
        """Update footer text."""
        self.footer_text = Text(text, justify="center", style="dim")
        self.layout["footer"].update(Panel(self.footer_text, border_style="dim"))

    def log(self, message: str):
        """Log a message (appears above UI via console.log, and in the log panel via handler)."""
        # The RichLogHandler attached to the root logger will capture this
        logging.info(message) # Example: Use standard logging
        # We can keep console.log if we want messages *also* above the live UI.
        self.console.log(message)

    def display_stats(self, stats: Dict[str, Any]) -> None:
        """Displays final statistics (retained for compatibility)."""
        # The new UI is live, but we can print a final summary table.
        self.console.log("--- Final Statistics ---")
        self.console.log(f"Total Bytes Transferred: {self._stats['transferred_bytes'] / (1024**3):.2f} GB")
        self.console.log(f"Successful Transfers: {self._stats['completed_transfers']}")
        self.console.log(f"Failed Transfers: {self._stats['failed_transfers']}")
        self.console.log(f"Total Duration: {time.time() - self._stats['start_time']:.2f} seconds")
