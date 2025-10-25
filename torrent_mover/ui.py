# torrent_mover/ui.py

import logging
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
from rich.logging import RichHandler as RichLogHandlerBase # Rename to avoid conflict


# Custom Log Handler to capture messages for the UI panel
class RichLogHandler(RichLogHandlerBase):
    def __init__(self, max_lines=15, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_lines = max_lines
        self.log_deque = deque(maxlen=max_lines)

    def emit(self, record: logging.LogRecord) -> None:
        message = self.format(record)
        renderable = self.render_message(record, message)
        self.log_deque.append(renderable)

    def get_renderables(self) -> Group:
        return Group(*list(self.log_deque))


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

        # --- CORRECTED ORDER: Define all attributes first ---
        # Data structures
        self._torrents: OrderedDict[str, Dict] = OrderedDict()
        self._active_torrents: Deque[str] = deque(maxlen=5) # Last 5 active
        self._completed_hashes: set = set()
        self._completed_torrents_log: Deque[Tuple[str, bool]] = deque(maxlen=10) # Store (name, success)

        # Logging
        self.log_handler = RichLogHandler(max_lines=15, show_path=False, markup=True, rich_tracebacks=True) # Keep last 15 log lines
        # Do NOT add handler here; it will be added in setup_logging in main script

        self.log_renderables = Group() # Initialize log renderables

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
            Layout(name="logs", ratio=1, minimum_size=5) # Add logs panel below, ensure min size
        )

        # Split right panel vertically
        self.layout["right_panel"].split(
            Layout(name="stats", ratio=1, minimum_size=8), # Ensure min size for stats
            Layout(name="completed", ratio=1, minimum_size=5) # Add completed panel below, ensure min size
        )

        # Initialize components (Now safe to call setup methods)
        self._setup_header(version)
        self._setup_progress()
        self._setup_stats_panel()
        self._setup_log_panel() # New
        self._setup_completed_panel() # New
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
        self.layout["logs"].update(
            Panel(self.log_renderables, title="[bold]Logs (Last 15)[/bold]", border_style="yellow")
        )

    def _setup_completed_panel(self):
        """Setup the completed torrents panel."""
        # Table columns already defined in __init__
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

    def _update_log_display(self):
        """Update the log display panel."""
        with self._lock:
            # Get latest renderables from handler
            self.log_renderables = self.log_handler.get_renderables()
            self.layout["logs"].update(
                Panel(self.log_renderables, title="[bold]Logs (Last 15)[/bold]", border_style="yellow")
            )

    def _update_completed_display(self):
        """Update the completed torrents table."""
        with self._lock:
            # Clear and rebuild table
            self.completed_table = Table.grid(padding=(0, 1))
            self.completed_table.add_column("Status", width=4) # Icon column
            self.completed_table.add_column("Torrent Name")
            for name, success in reversed(self._completed_torrents_log): # Show newest first
                status_icon = "[green]✓[/]" if success else "[bold red]✗[/]"
                self.completed_table.add_row(status_icon, Text(name[:50], overflow="ellipsis")) # Truncate long names

            # Ensure there's some content if empty, prevents collapse
            if not self._completed_torrents_log:
                self.completed_table.add_row(" ", Text(" ", justify="center")) # Placeholder row

            self.layout["completed"].update(
                Panel(self.completed_table, title="[bold]Recently Completed (Last 10)", border_style="dim")
            )

    def __enter__(self):
        self._live = Live(
            self.layout,
            console=self.console,
            screen=True,
            refresh_per_second=2 # Reduced from 10 (less CPU)
        )
        self._live.start()

        # Start stats update thread
        self._stats_thread_stop.clear() # Ensure stop event is clear before starting
        self._stats_thread = threading.Thread(target=self._stats_updater, daemon=True)
        self._stats_thread.start()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Stop the stats update thread gracefully
        if self._stats_thread:
            self._stats_thread_stop.set()
            self._stats_thread.join(timeout=2) # Wait briefly for thread to exit

        if self._live:
            self.main_progress.stop()
            self.files_progress.stop()
            self._live.stop()

        # Remove the log handler we added
        logging.getLogger().removeHandler(self.log_handler)

    def _stats_updater(self):
        """Background thread to update stats, logs, and completed panels."""
        while not self._stats_thread_stop.is_set():
            time.sleep(1) # Update interval
            # Check if live display is still active before updating
            if self._live and self._live.is_started and not self._stats_thread_stop.is_set():
                self._update_stats_display()
                self._update_log_display()
                self._update_completed_display()
            else:
                break # Exit if live display stopped or stop event set

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
        # Only show last 5 files
        if len(self.active_file_tasks) >= 5:
            # Remove oldest
            try:
                oldest_file = next(iter(self.active_file_tasks))
                task_id = self.active_file_tasks.pop(oldest_file)
                self.files_progress.remove_task(task_id)
            except StopIteration:
                pass # No tasks to remove

        # Add new file
        file_name = file_path.split('/')[-1]
        task_id = self.files_progress.add_task(
            file_name[:50],
            total=file_size
        )
        self.active_file_tasks[file_path] = task_id

    def update_file_progress(self, file_path: str, bytes_transferred: int):
        """Update progress for a specific file."""
        if file_path in self.active_file_tasks:
            self.files_progress.update(
                self.active_file_tasks[file_path],
                advance=bytes_transferred
            )

    def complete_file_transfer(self, file_path: str):
        """Mark a file as completed."""
        if file_path in self.active_file_tasks:
            task_id = self.active_file_tasks.pop(file_path)
            self.files_progress.remove_task(task_id)

    def complete_torrent_transfer(self, torrent_hash: str, success: bool = True):
        """Mark a torrent as completed."""
        with self._lock:
            if torrent_hash in self._torrents:
                self._torrents[torrent_hash]["status"] = "completed" if success else "failed"
                self._completed_hashes.add(torrent_hash)
                self._stats["active_transfers"] = max(0, self._stats["active_transfers"] - 1)

                if success:
                    self._stats["completed_transfers"] += 1
                else:
                    self._stats["failed_transfers"] += 1

        # Hide current torrent progress
        self.main_progress.update(self.current_torrent_task, visible=False, description="[yellow]Current: (none)")

    def update_header(self, text: str):
        """Update header text."""
        self.header_text = Text(text, justify="center", style="bold magenta")
        self.layout["header"].update(Panel(self.header_text, border_style="magenta"))

    def update_footer(self, text: str):
        """Update footer text."""
        self.footer_text = Text(text, justify="center", style="dim")
        self.layout["footer"].update(Panel(self.footer_text, border_style="dim"))

    def log(self, message: str):
        """Log a message (appears above UI)."""
        self.console.log(message)

    def display_stats(self, stats: Dict[str, Any]) -> None:
        """Displays final statistics (retained for compatibility)."""
        # The new UI is live, but we can print a final summary table.
        self.console.log("--- Final Statistics ---")
        self.console.log(f"Total Bytes Transferred: {self._stats['transferred_bytes'] / (1024**3):.2f} GB")
        self.console.log(f"Successful Transfers: {self._stats['completed_transfers']}")
        self.console.log(f"Failed Transfers: {self._stats['failed_transfers']}")
        self.console.log(f"Total Duration: {time.time() - self._stats['start_time']:.2f} seconds")
