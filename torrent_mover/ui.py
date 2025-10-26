from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
Progress,
TextColumn,
BarColumn,
TransferSpeedColumn,
TimeRemainingColumn,
DownloadColumn, # <-- IMPORTED
TaskID,
)
from rich.table import Table
from rich.text import Text
from rich.align import Align
import threading
from collections import OrderedDict, deque
from typing import Dict, Any, Optional, Deque
from rich.layout import Layout
import time


class UIManagerV2:
    """
    Enhanced UI with improved information density and visual clarity.
    v3: Fixes color theme, adds file sizes, and corrects progress display.
    """

    def __init__(self, version: str = ""):
        self.console = Console()
        self._lock = threading.RLock()
        self._live: Optional[Live] = None
        self.version = version
        self.transfer_mode = "" # For transfer mode
        self._log_buffer: Deque[Text] = deque(maxlen=20) # For log panel

        # Data structures
        self._torrents: OrderedDict[str, Dict] = OrderedDict()
        self._active_torrents: Deque[str] = deque(maxlen=5)
        self._completed_hashes: set = set()
        self._recent_completions: Deque[tuple] = deque(maxlen=5) # (name, size, duration)

        # Statistics
        self._stats = {
            "total_torrents": 0,
            "total_bytes": 0,
            "transferred_bytes": 0,
            "start_time": time.time(),
            "active_transfers": 0,
            "completed_transfers": 0,
            "failed_transfers": 0,
            "peak_speed": 0.0,
            "current_speed": 0.0,
        }
        self._speed_history: Deque[float] = deque(maxlen=30) # Last 30 seconds

        # Create layout with better proportions
        self.layout = Layout()
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=7) # Increased for log
        )

        # Split body into three columns
        self.layout["body"].split_row(
            Layout(name="left", ratio=2), # Progress bars
            Layout(name="middle", ratio=1), # Current torrents
            Layout(name="right", ratio=1) # Stats + Recent
        )

        # Initialize components
        self._setup_header()
        self._setup_progress()
        self._setup_current_torrents()
        self._setup_stats_panel()
        self._setup_footer() # For log panel

    def _setup_header(self):
        """Enhanced header with version and mode indicators."""
        mode_str = f"[dim]({self.transfer_mode.upper()})[/dim]" if self.transfer_mode else ""
        self.header_text = Text(
            f"🚀 Torrent Mover v{self.version} {mode_str} - [green]Initializing...[/]",
            justify="center",
            style="bold magenta"
        )
        self.layout["header"].update(
            Panel(
                Align.center(self.header_text),
                title="[bold magenta]TORRENT MOVER[/]",
                border_style="dim",
                style="on #1a1a2e"
            )
        )

    def _setup_progress(self):
        """Setup progress bars with better formatting."""
        self.main_progress = Progress(
            TextColumn("[bold]{task.description}", justify="left"),
            BarColumn(bar_width=None, complete_style="green", finished_style="bold green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            "•",
            DownloadColumn(binary_units=True),
            "•",
            TransferSpeedColumn(),
            "•",
            TimeRemainingColumn(),
            expand=True,
        )

        # Analysis progress
        self.analysis_task = self.main_progress.add_task(
            "[cyan]📊 Analysis", total=100, visible=False
        )

        # Overall transfer progress
        self.overall_task = self.main_progress.add_task(
            "[green]📦 Overall Progress", total=100, visible=False
        )

        # Current torrent progress
        self.current_torrent_task = self.main_progress.add_task(
            "[yellow]⚡ Current Torrent: (none)", total=100, visible=False
        )

        # Active files progress
        self.files_progress = Progress(
            TextColumn(" [dim]└─[/] {task.description}", justify="left"),
            BarColumn(bar_width=15, complete_style="cyan"),
            TextColumn("{task.percentage:>3.0f}%"),
            "•",
            DownloadColumn(binary_units=True),
            "•",
            TransferSpeedColumn(),
            expand=True,
        )
        self.active_file_tasks: Dict[str, Tuple[TaskID, str]] = {} # Store (task_id, torrent_hash)

        # Combine into left panel
        progress_group = Group(
            Panel(
                self.main_progress,
                title="[bold green]📈 Transfer Progress",
                border_style="dim",
                style="on #16213e"
            ),
            Panel(
                self.files_progress,
                title="[bold cyan]📄 Active Files (Last 5)",
                border_style="dim",
                height=9,
                style="on #0f3460"
            )
        )

        self.layout["left"].update(progress_group)

    def _setup_current_torrents(self):
        """Setup middle panel showing current torrent queue."""
        self.current_table = Table.grid(padding=(0, 1))
        self.current_table.add_column(style="bold cyan", no_wrap=True)
        self.current_table.add_column()

        self._update_current_torrents()

        self.layout["middle"].update(
            Panel(
                self.current_table,
                title="[bold yellow]🎯 Active Queue",
                border_style="dim",
                style="on #16213e"
            )
        )

    def _update_current_torrents(self):
        """Update the current torrents display."""
        with self._lock:
            self.current_table = Table.grid(padding=(0, 1))
            self.current_table.add_column(style="bold", no_wrap=True, width=12)
            self.current_table.add_column(style="dim")

            active_count = 0
            for hash_, torrent in self._torrents.items():
                if torrent["status"] == "transferring":
                    active_count += 1
                    name = torrent["name"]
                    # Truncate long names
                    display_name = name[:30] + "..." if len(name) > 33 else name
                    progress = torrent["transferred"] / torrent["size"] * 100 if torrent["size"] > 0 else 0

                    # Added file counts from Task 6
                    files_str = f"({torrent.get('completed_files', 0)}/{torrent.get('total_files', 0)} files)"

                    status_icon = "🔄"
                    self.current_table.add_row(
                        f"{status_icon} {progress:.0f}%",
                        f"{display_name} [dim]{files_str}[/dim]" # <-- ADDED
                    )

            if active_count == 0:
                self.current_table.add_row("[dim]⏸️ Idle", "[dim]Waiting for torrents...")

            # Add queued count
            queued = self._stats["total_torrents"] - self._stats["completed_transfers"] - self._stats["failed_transfers"] - active_count
            if queued > 0:
                self.current_table.add_row("", "")
                self.current_table.add_row("[bold]⏳ Queued:", f"[yellow]{queued} torrent(s)[/]")

            self.layout["middle"].update(
                Panel(
                    self.current_table,
                    title="[bold yellow]🎯 Active Queue",
                    border_style="dim",
                    style="on #16213e" # <-- ADDED DARK THEME
                )
            )

    def _setup_stats_panel(self):
        """Enhanced stats panel with graphs and recent completions."""
        self._update_stats_display()

    def _update_stats_display(self):
        """Update the stats display with speed history."""
        with self._lock:
            elapsed = time.time() - self._stats["start_time"]
            # Use average speed for a more stable "current" speed
            self._speed_history.append(self._stats["current_speed"]) # current_speed is updated in update_torrent_progress
            avg_speed = sum(self._speed_history) / len(self._speed_history) if self._speed_history else 0

            # Track peak speed
            if avg_speed > self._stats["peak_speed"]:
                self._stats["peak_speed"] = avg_speed

            # Create stats table
            stats_table = Table.grid(padding=(0, 2))
            stats_table.add_column(style="bold cyan", justify="right", no_wrap=True) # <-- Standardized color
            stats_table.add_column()

            # Transfer stats - Added from Task 1
            transferred_gb = self._stats['transferred_bytes'] / (1024**3)
            total_gb = self._stats['total_bytes'] / (1024**3)
            remaining_gb = max(0, total_gb - transferred_gb)

            stats_table.add_row("📊 Transferred:", f"[white]{transferred_gb:.2f} / {total_gb:.2f} GB[/white]")
            stats_table.add_row("⏳ Remaining:", f"[white]{remaining_gb:.2f} GB[/white]")
            stats_table.add_row("⚡ Speed:", f"[white]{avg_speed / (1024**2):.2f} MB/s[/white]")
            stats_table.add_row("🔥 Peak Speed:", f"[dim]{self._stats['peak_speed'] / (1024**2):.2f} MB/s[/dim]")

            stats_table.add_row("", "") # Spacer

            # Status stats
            stats_table.add_row("🔄 Active:", f"[white]{self._stats['active_transfers']}[/white]")
            stats_table.add_row("✅ Completed:", f"[white]{self._stats['completed_transfers']}[/white]")
            stats_table.add_row("❌ Failed:", f"[white]{self._stats['failed_transfers']}[/white]")

            # File stats - Added from Task 6
            total_files_overall = sum(t.get('total_files', 0) for t in self._torrents.values())
            completed_files_overall = sum(t.get('completed_files', 0) for t in self._torrents.values())
            stats_table.add_row("📂 Files:", f"[white]{completed_files_overall} / {total_files_overall}[/white]")

            stats_table.add_row("", "") # Spacer

            # Time stats
            hours = int(elapsed // 3600)
            minutes = int((elapsed % 3600) // 60)
            seconds = int(elapsed % 60)
            time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            stats_table.add_row("⏱️ Elapsed:", f"[dim]{time_str}[/dim]")

            # ETA calculation
            if remaining_gb > 0 and avg_speed > 0:
                eta_seconds = (remaining_gb * 1024**3) / avg_speed
                eta_hours = int(eta_seconds // 3600)
                eta_minutes = int((eta_seconds % 3600) // 60)
                eta_str = f"{eta_hours:02d}:{eta_minutes:02d}"
                stats_table.add_row("⏳ ETA:", f"[cyan]{eta_str}[/]")

            # Recent completions section
            if self._recent_completions:
                recent_table = Table.grid(padding=(0, 1))
                recent_table.add_column(style="dim", no_wrap=True)
                recent_table.add_column(style="dim")

                for name, size, duration in list(self._recent_completions)[-3:]:
                    display_name = name[:25] + "..." if len(name) > 28 else name
                    speed = size / duration if duration > 0 else 0
                    recent_table.add_row(
                        f"✓ {display_name}",
                        f"{speed / (1024**2):.1f} MB/s"
                    )

                stats_group = Group(
                    Panel(stats_table, title="[bold cyan]📊 Statistics", border_style="dim", style="on #0f3460"),
                    Panel(recent_table, title="[bold green]🎉 Recent Completions", border_style="dim", style="on #16213e")
                )
            else:
                stats_group = Panel(stats_table, title="[bold cyan]📊 Statistics", border_style="dim", style="on #0f3460")

            self.layout["right"].update(stats_group)

    def _setup_footer(self):
        """Initial setup for the log panel footer."""
        log_panel = Panel(
            Align.left("[dim]Log display initialized...[/]"),
            title="[bold]📜 Live Log",
            border_style="dim",
            style="on #0a0e27"
        )
        self.layout["footer"].update(log_panel)

    def _update_footer_display(self):
        """Updates the footer with the latest log messages."""
        with self._lock:
            # Join the deque of Text objects with newlines
            log_text = Text("\n").join(list(self._log_buffer))

            self.layout["footer"].update(
                Panel(
                    Align.left(log_text),
                    title="[bold]📜 Live Log",
                    border_style="dim",
                    style="on #0a0e27"
                )
            )

    def __enter__(self):
        self._live = Live(
            self.layout,
            console=self.console,
            screen=True,
            redirect_stderr=False,
            refresh_per_second=4
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
            if self._stats_thread.is_alive():
                self._stats_thread.join()
            self.main_progress.stop()
            self.files_progress.stop()
            self._live.stop()

    def _stats_updater(self):
        """Background thread to update stats and displays."""
        while not self._stats_thread_stop.wait(1.0):
            with self._lock:
                # Calculate overall speed
                active_speed = 0.0
                for task in self.files_progress.tasks:
                    active_speed += task.speed if task.speed is not None else 0
                self._stats["current_speed"] = active_speed

                self._update_stats_display()
                self._update_current_torrents()
                self._update_footer_display() # For log panel

    # ===== Public API (keeping existing methods) =====

    def set_transfer_mode(self, mode: str):
        """Sets the transfer mode string to display in the header."""
        with self._lock:
            self.transfer_mode = mode
            self._setup_header() # Redraw header

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

    def start_torrent_transfer(self, torrent_hash: str, torrent_name: str,
                               total_size: float, total_files: int,
                               transfer_multiplier: int = 1):
        with self._lock:
            self._torrents[torrent_hash] = {
                "name": torrent_name,
                "size": total_size * transfer_multiplier,
                "total_files": total_files,
                "completed_files": 0,
                "transferred": 0,
                "status": "transferring",
                "start_time": time.time()
            }
            self._active_torrents.append(torrent_hash)
            self._stats["active_transfers"] += 1

            # Update current torrent progress bar
            display_name = torrent_name[:40] + "..." if len(torrent_name) > 40 else torrent_name
            self.main_progress.update(
                self.current_torrent_task,
                description=f"[yellow]⚡ Current Torrent: {display_name}", # <-- RENAMED
                completed=0,
                total=total_size * transfer_multiplier,
                visible=True
            )

    def update_torrent_progress(self, torrent_hash: str, bytes_transferred: float):
        with self._lock:
            if torrent_hash in self._torrents:
                self._torrents[torrent_hash]["transferred"] += bytes_transferred
                self._stats["transferred_bytes"] += bytes_transferred

            # Update progress bars
            self.main_progress.update(self.overall_task, advance=bytes_transferred)
            self.main_progress.update(self.current_torrent_task, advance=bytes_transferred)

    def start_file_transfer(self, torrent_hash: str, file_path: str, file_size: int):
        # Only show last 5 files
        if len(self.active_file_tasks) >= 5:
            try:
                oldest_file = next(iter(self.active_file_tasks))
                task_id, _ = self.active_file_tasks.pop(oldest_file)
                self.files_progress.remove_task(task_id)
            except StopIteration:
                pass
            except Exception:
                pass # Failsafe

        # Add new file with icon
        file_name = file_path.split('/')[-1]
        display_name = file_name[:45] + "..." if len(file_name) > 45 else file_name
        task_id = self.files_progress.add_task(
            f"📄 {display_name}",
            total=file_size
        )
        self.active_file_tasks[file_path] = (task_id, torrent_hash) # Store hash

    def update_file_progress(self, file_path: str, bytes_transferred: int):
        if file_path in self.active_file_tasks:
            task_id, _ = self.active_file_tasks[file_path]
            self.files_progress.update(
                task_id,
                advance=bytes_transferred
            )

    def complete_file_transfer(self, file_path: str):
        if file_path in self.active_file_tasks:
            task_id, torrent_hash = self.active_file_tasks.pop(file_path)
            # Finish the progress bar
            self.files_progress.update(task_id, completed=self.files_progress.tasks[task_id].total)
            self.files_progress.remove_task(task_id)
            with self._lock:
                if torrent_hash in self._torrents:
                    self._torrents[torrent_hash]["completed_files"] += 1

    def complete_torrent_transfer(self, torrent_hash: str, success: bool = True):
        with self._lock:
            if torrent_hash in self._torrents:
                torrent = self._torrents[torrent_hash]
                torrent["status"] = "completed" if success else "failed"
                self._completed_hashes.add(torrent_hash)
                self._stats["active_transfers"] = max(0, self._stats["active_transfers"] - 1)

                if success:
                    self._stats["completed_transfers"] += 1
                    # Add to recent completions
                    duration = time.time() - torrent.get("start_time", time.time())
                    self._recent_completions.append((
                        torrent["name"],
                        torrent["size"],
                        duration
                    ))
                else:
                    self._stats["failed_transfers"] += 1

                # Hide current torrent progress
                self.main_progress.update(self.current_torrent_task, visible=False, description="[yellow]⚡ Current Torrent: (none)")

    def update_header(self, text: str):
        mode_str = f"[dim]({self.transfer_mode.upper()})[/dim]" if self.transfer_mode else ""
        self.header_text = Text(f"🚀 Torrent Mover v{self.version} {mode_str} - {text}", justify="center", style="bold magenta")
        self.layout["header"].update(Panel(Align.center(self.header_text), title="[bold magenta]TORRENT MOVER[/]", border_style="dim", style="on #1a1a2e"))

    def log(self, message: str, style: str = "dim"):
        """Adds a message to the on-screen log buffer."""
        with self._lock:
            # Format with a timestamp
            timestamp = time.strftime("%H:%M:%S")
            self._log_buffer.append(Text(f"[{timestamp}] {message}", style=style))

    def display_stats(self, stats: Dict[str, Any]) -> None:
        """Displays final statistics."""
        # This is less important now as stats are live
        self.console.print("--- Final Statistics ---", style="bold green")
        self.console.print(f"Total Bytes Transferred: {self._stats['transferred_bytes'] / (1024**3):.2f} GB")
        self.console.print(f"Peak Speed: {self._stats['peak_speed'] / (1024**2):.2f} MB/s")
        self.console.print(f"Successful Transfers: {self._stats['completed_transfers']}")
        self.console.print(f"Failed Transfers: {self._stats['failed_transfers']}")
        self.console.print(f"Total Duration: {time.time() - self._stats['start_time']:.2f} seconds")
