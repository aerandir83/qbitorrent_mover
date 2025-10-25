# torrent_mover/ui.py

from rich.console import Console, Group, ConsoleOptions, RenderResult
from rich.live import Live
from rich.measure import Measurement
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
)
from rich.table import Table, Column
from rich.text import Text
import threading
from collections import OrderedDict
from typing import Dict, Any, List, Optional, OrderedDict as OrderedDictType
from rich.layout import Layout
from rich.progress import (
    DownloadColumn,
)
from collections import deque
import time
from typing import Deque

class TorrentTableRenderable:
    """A renderable to display the torrents table."""

    def __init__(self, torrents_data: OrderedDictType[str, Dict[str, Any]], lock: threading.RLock):
        self._torrents_data = torrents_data
        self._lock = lock

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        """Builds the torrents table renderable."""
        with self._lock:
            main_grid = Table.grid(expand=True)
            main_grid.add_column("Details")

            for torrent_hash, data in self._torrents_data.items():
                torrent_grid = Table.grid(expand=True)
                header_table = Table(show_header=False, box=None, padding=0, expand=True)
                header_table.add_column()
                header_table.add_column(justify="right")
                header_table.add_row(f"[bold cyan]{data['name']}[/bold cyan]", f"[magenta]{data['size']}[/magenta]")
                torrent_grid.add_row(header_table)

                if data.get("progress_obj"):
                    torrent_grid.add_row(data["progress_obj"])
                elif data.get("status_text"):
                    torrent_grid.add_row(Panel(Text.from_markup(data['status_text'], justify="center"), style="dim"))

                if data["files"]:
                    files_table = Table(show_header=True, header_style="bold blue", box=None, padding=(0, 1), expand=True)
                    files_table.add_column("File", no_wrap=True)
                    files_table.add_column("Progress", no_wrap=True)
                    for file_path, file_data in data["files"].items():
                        file_name = file_path.split('/')[-1]
                        progress_renderable = file_data.get("progress_obj") or Text.from_markup(file_data["status"], justify="center")
                        files_table.add_row(file_name, progress_renderable)
                    torrent_grid.add_row(Panel(files_table, style="blue", border_style="dim"))

                main_grid.add_row(Panel(torrent_grid, border_style="cyan" if data.get("progress_obj") else "dim"))

            yield main_grid

class UIManager:
    """A class to manage the Rich UI for the torrent mover script."""

    def __init__(self, version: str = "") -> None:
        self.console = Console()
        self._lock = threading.RLock()
        self._live: Optional[Live] = None
        self._torrents_data: OrderedDictType[str, Dict[str, Any]] = OrderedDict()

        self.header_text = Text("Initializing...", justify="center")
        self.header_panel = Panel(self.header_text, title=f"[bold magenta]Torrent Mover v{version}[/bold magenta]", border_style="magenta")

        self.analysis_progress = Progress(TextColumn("[cyan]Analyzed"), BarColumn(), MofNCompleteColumn())
        self.analysis_task: TaskID = self.analysis_progress.add_task("Torrents", total=0, visible=False)

        self.transfer_progress = Progress(TextColumn("[blue]Completed"), BarColumn(), MofNCompleteColumn())
        self.transfer_task: TaskID = self.transfer_progress.add_task("Torrents", total=0, visible=False)

        self.overall_progress = Progress(TextColumn("[green]Overall"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), FileSizeColumn(), TextColumn("/"), TotalFileSizeColumn(), TransferSpeedColumn(), TimeRemainingColumn())
        self.overall_task: TaskID = self.overall_progress.add_task("Total", total=0, visible=False)

        run_progress_group = Group(self.analysis_progress, self.transfer_progress, self.overall_progress)
        self.run_progress_panel = Panel(run_progress_group, title="[bold]Run Progress[/bold]", border_style="green")

        self.torrents_table_renderable = TorrentTableRenderable(self._torrents_data, self._lock)
        self.torrents_table_panel = Panel(self.torrents_table_renderable, title="[bold]Transfer Queue[/bold]", border_style="cyan")

        self.footer_text = Text("Waiting to start...", justify="center")
        self.footer_panel = Panel(self.footer_text, border_style="dim")

        self.layout = Table.grid(expand=True)
        self.layout.add_row(self.header_panel)
        self.layout.add_row(self.run_progress_panel)
        self.layout.add_row(self.torrents_table_panel)
        self.layout.add_row(self.footer_panel)

    def __enter__(self) -> "UIManager":
        self._live = Live(self.layout, console=self.console, screen=True, redirect_stderr=False, refresh_per_second=10)
        self._live.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._live:
            # Force stop ALL progress objects, even if UI crashed
            with self._lock:
                for data in self._torrents_data.values():
                    # Stop main torrent-level progress objects
                    for key in ['progress_obj', 'byte_progress_obj', 'file_progress_obj']:
                        progress_item = data.get(key)
                        if isinstance(progress_item, Progress):
                            try:
                                progress_item.stop()
                            except Exception:
                                pass # Ignore errors during shutdown
                        elif isinstance(progress_item, Group):
                             # Handle the group containing byte/file progress
                             for sub_progress in progress_item.renderables:
                                 if isinstance(sub_progress, Progress):
                                     try:
                                         sub_progress.stop()
                                     except Exception:
                                         pass

                    # Stop all file-level progress objects
                    if "files" in data:
                        for file_data in data["files"].values():
                            file_progress = file_data.get("progress_obj")
                            if isinstance(file_progress, Progress):
                                try:
                                    file_progress.stop()
                                except Exception:
                                    pass

            # Stop the main live display
            self._live.stop()

    def log(self, message: str) -> None:
        """Logs a message to the Rich console, appearing above the live display."""
        self.console.log(message)

    def set_analysis_total(self, total: int) -> None:
        self.analysis_progress.update(self.analysis_task, total=total, visible=True)
        self.transfer_progress.update(self.transfer_task, total=total, visible=True)

    def advance_analysis_progress(self) -> None:
        self.analysis_progress.update(self.analysis_task, advance=1)

    def advance_transfer_progress(self) -> None:
        self.transfer_progress.update(self.transfer_task, advance=1)

    def set_overall_total(self, total: float) -> None:
        self.overall_progress.update(self.overall_task, total=total, visible=True)

    def advance_overall_progress(self, advance: float) -> None:
        self.overall_progress.update(self.overall_task, advance=advance)

    def update_header(self, text: str) -> None:
        self.header_panel.renderable = Text(text, justify="center")

    def update_footer(self, text: str) -> None:
        self.footer_panel.renderable = Text(text, justify="center")

    def add_torrent_to_plan(self, torrent_name: str, torrent_hash: str, size_str: str) -> None:
        """Adds a torrent to the UI table with 'Queued' status."""
        with self._lock:
            if torrent_hash in self._torrents_data:
                return
            self._torrents_data[torrent_hash] = {
                "name": torrent_name,
                "size": size_str,
                "status_text": "[dim]Queued[/dim]",
                "progress_obj": None,
                "files": OrderedDict(),
            }

    def update_torrent_status_text(self, torrent_hash: str, status: str, color: str = "yellow") -> None:
        """Updates the status text of a specific torrent, used for non-transfer states."""
        with self._lock:
            if torrent_hash in self._torrents_data:
                self._torrents_data[torrent_hash].update({
                    "status_text": f"[{color}]{status}[/{color}]",
                    "progress_obj": None,
                })

    def start_torrent_transfer(self, torrent_hash: str, total_size: float, files_to_transfer: List[str], transfer_multiplier: int = 1) -> None:
        with self._lock:
            if torrent_hash not in self._torrents_data:
                return
            byte_progress = Progress(
                TextColumn("[bold blue]Bytes[/bold blue]"), BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), "•", FileSizeColumn(), TextColumn("/"), TotalFileSizeColumn(),
            )
            byte_task = byte_progress.add_task("Bytes", total=total_size * transfer_multiplier)
            file_progress = Progress(
                TextColumn("[bold blue]Files[/bold blue]"), BarColumn(), MofNCompleteColumn(),
            )
            file_task = file_progress.add_task("Files", total=len(files_to_transfer))
            progress_group = Group(byte_progress, file_progress)
            self._torrents_data[torrent_hash].update({
                "progress_obj": progress_group,
                "byte_progress_obj": byte_progress,
                "file_progress_obj": file_progress,
                "byte_task_id": byte_task,
                "file_task_id": file_task,
                "status_text": None,
                "files": OrderedDict((f, {"status": "", "progress_obj": None}) for f in files_to_transfer),
            })

    def update_torrent_byte_progress(self, torrent_hash: str, advance: float) -> None:
        with self._lock:
            data = self._torrents_data.get(torrent_hash)
            if data and data.get("byte_progress_obj"):
                data["byte_progress_obj"].update(data["byte_task_id"], advance=advance)

    def advance_torrent_file_progress(self, torrent_hash: str) -> None:
        with self._lock:
            data = self._torrents_data.get(torrent_hash)
            if data and data.get("file_progress_obj"):
                data["file_progress_obj"].update(data["file_task_id"], advance=1)

    def start_file_transfer(self, torrent_hash: str, file_path: str, file_size: int) -> None:
        with self._lock:
            data = self._torrents_data.get(torrent_hash)
            if not data or file_path not in data["files"]:
                return
            progress = Progress(
                TextColumn("{task.description}", style="blue", table_column=Column(justify="right", width=12)),
                BarColumn(bar_width=30),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%", table_column=Column(justify="left", width=5)),
                "•",
                TransferSpeedColumn(table_column=Column(justify="left", width=10)),
                "•",
                FileSizeColumn(table_column=Column(justify="right", width=10)),
                TextColumn("/", table_column=Column(justify="center", width=1)),
                TotalFileSizeColumn(table_column=Column(justify="left", width=10))
            )
            task_id = progress.add_task("Starting...", total=file_size)
            data["files"][file_path] = {
                "progress_obj": progress,
                "task_id": task_id,
                "status": "Starting..."
            }

    def update_file_status(self, torrent_hash: str, file_path: str, status: str) -> None:
        with self._lock:
            data = self._torrents_data.get(torrent_hash)
            if data and file_path in data["files"] and data["files"][file_path].get("progress_obj"):
                file_data = data["files"][file_path]
                file_data["progress_obj"].update(file_data["task_id"], description=status)
                file_data["status"] = status

    def update_file_progress(self, torrent_hash: str, file_path: str, advance: int) -> None:
        with self._lock:
            data = self._torrents_data.get(torrent_hash)
            if data and file_path in data["files"] and data["files"][file_path].get("progress_obj"):
                data["files"][file_path]["progress_obj"].update(data["files"][file_path]["task_id"], advance=advance)

    def stop_torrent_transfer(self, torrent_hash: str, success: bool = True) -> None:
        with self._lock:
            if torrent_hash in self._torrents_data:
                data = self._torrents_data[torrent_hash]
                if data.get("byte_progress_obj"):
                    data["byte_progress_obj"].stop()
                if data.get("file_progress_obj"):
                    data["file_progress_obj"].stop()
                for file_data in data["files"].values():
                    if isinstance(file_data.get("progress_obj"), Progress):
                        file_data["progress_obj"].stop()
                if success:
                    self.update_torrent_status_text(torrent_hash, "Completed", color="green")
                else:
                    self.update_torrent_status_text(torrent_hash, "Failed", color="bold red")

    def display_stats(self, stats: Dict[str, Any]) -> None:
        """Displays the final transfer statistics."""
        table = Table(title="[bold]Transfer Statistics[/bold]", show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="dim")
        table.add_column("Value", justify="right")
        table.add_row("Total Bytes Transferred", f"{stats['total_bytes_transferred'] / (1024**3):.2f} GB")
        table.add_row("Disk Space Saved on Source", f"{stats['total_bytes_transferred'] / (1024**3):.2f} GB")
        table.add_row("Successful Transfers", str(stats['successful_transfers']))
        table.add_row("Failed Transfers", str(stats['failed_transfers']))
        table.add_row("Total Duration", f"{stats['duration']:.2f} seconds")
        if stats["total_transfer_time"] > 0:
            average_speed = stats["total_bytes_transferred"] / stats["total_transfer_time"]
            table.add_row("Average Transfer Speed", f"{average_speed / (1024*1024):.2f} MB/s")
        if stats["torrent_transfer_times"]:
            min_time = min(stats["torrent_transfer_times"])
            max_time = max(stats["torrent_transfer_times"])
            avg_time = sum(stats["torrent_transfer_times"]) / len(stats["torrent_transfer_times"])
            table.add_row("Torrent Transfer Time (min/avg/max)", f"{min_time:.2f}s / {avg_time:.2f}s / {max_time:.2f}s")
        self.console.print(table)


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

        # Create layout
        self.layout = Layout()
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=3)
        )

        # Split body into left (torrents) and right (stats)
        self.layout["body"].split_row(
            Layout(name="torrents", ratio=2),
            Layout(name="stats", ratio=1)
        )

        # Initialize components
        self._setup_header(version)
        self._setup_progress()
        self._setup_stats_panel()
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
            self._stats_thread.join()
            self.main_progress.stop()
            self.files_progress.stop()
            self._live.stop()

    def _stats_updater(self):
        """Background thread to update stats every second."""
        while not self._stats_thread_stop.wait(1.0):
            self._update_stats_display()

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