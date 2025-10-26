# torrent_mover/ui.py - Enhanced Version

from rich.console import Console, Group
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
    """

    def __init__(self, version: str = ""):
        self.console = Console()
        self._lock = threading.RLock()
        self._live: Optional[Live] = None

        # Data structures
        self._torrents: OrderedDict[str, Dict] = OrderedDict()
        self._active_torrents: Deque[str] = deque(maxlen=5)
        self._completed_hashes: set = set()
        self._recent_completions: Deque[tuple] = deque(maxlen=5)  # (name, size, duration)

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
        self._speed_history: Deque[float] = deque(maxlen=30)  # Last 30 seconds

        # Create layout with better proportions
        self.layout = Layout()
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=5)  # Increased for more info
        )

        # Split body into three columns
        self.layout["body"].split_row(
            Layout(name="left", ratio=2),  # Progress bars
            Layout(name="middle", ratio=1),  # Current torrents
            Layout(name="right", ratio=1)  # Stats + Recent
        )

        # Initialize components
        self._setup_header(version)
        self._setup_progress()
        self._setup_current_torrents()
        self._setup_stats_panel()
        self._setup_footer()

    def _setup_header(self, version: str):
        """Enhanced header with version and mode indicators."""
        self.version = version
        self.header_text = Text(f"🚀 Torrent Mover v{version}", justify="center", style="bold magenta")
        self.layout["header"].update(
            Panel(self.header_text, border_style="dim", style="on #1a1a2e")
        )

    def _setup_progress(self):
        """Setup progress bars with better formatting."""
        self.main_progress = Progress(
            TextColumn("[bold]{task.description}", justify="left"),
            BarColumn(bar_width=None, complete_style="green", finished_style="bold green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            "•",
            DownloadColumn(),
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
            "[yellow]⚡ Current: (none)", total=100, visible=False
        )

        # Active files progress
        self.files_progress = Progress(
            TextColumn(" [dim]└─[/] {task.description}", justify="left"),
            BarColumn(bar_width=20, complete_style="cyan"),
            TextColumn("{task.percentage:>3.0f}%"),
            "•",
            TransferSpeedColumn(),
            expand=True,
        )
        self.active_file_tasks: Dict[str, TaskID] = {}

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

                    status_icon = "🔄"
                    self.current_table.add_row(
                        f"{status_icon} {progress:.0f}%",
                        display_name
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
                    style="on #16213e"
                )
            )

    def _setup_stats_panel(self):
        """Enhanced stats panel with graphs and recent completions."""
        self._update_stats_display()

    def _update_stats_display(self):
        """Update the stats display with speed history."""
        with self._lock:
            elapsed = time.time() - self._stats["start_time"]
            current_speed = self._stats["transferred_bytes"] / elapsed if elapsed > 1 else 0

            # Update speed history
            self._speed_history.append(current_speed)
            avg_speed = sum(self._speed_history) / len(self._speed_history) if self._speed_history else 0

            # Track peak speed
            if current_speed > self._stats["peak_speed"]:
                self._stats["peak_speed"] = current_speed

            # Create stats table
            stats_table = Table.grid(padding=(0, 2))
            stats_table.add_column(style="bold cyan", justify="right", no_wrap=True)
            stats_table.add_column()

            # Transfer stats
            transferred_gb = self._stats['transferred_bytes'] / (1024**3)
            total_gb = self._stats['total_bytes'] / (1024**3)
            remaining_gb = total_gb - transferred_gb
            stats_table.add_row("📊 Transferred:", f"[white]{transferred_gb:.2f} / {total_gb:.2f} GB[/white]")
            stats_table.add_row("⏳ Remaining:", f"[white]{remaining_gb:.2f} GB[/white]")
            stats_table.add_row("⚡ Speed:", f"[white]{current_speed / (1024**2):.2f} MB/s[/white]")
            stats_table.add_row("📈 Avg Speed:", f"[dim]{avg_speed / (1024**2):.2f} MB/s[/dim]")
            stats_table.add_row("🔥 Peak Speed:", f"[dim]{self._stats['peak_speed'] / (1024**2):.2f} MB/s[/dim]")

            stats_table.add_row("", "")  # Spacer

            # Status stats
            stats_table.add_row("🔄 Active:", f"[white]{self._stats['active_transfers']}[/white]")
            stats_table.add_row("✅ Completed:", f"[white]{self._stats['completed_transfers']}[/white]")
            stats_table.add_row("❌ Failed:", f"[white]{self._stats['failed_transfers']}[/white]")

            stats_table.add_row("", "")  # Spacer

            # Time stats
            hours = int(elapsed // 3600)
            minutes = int((elapsed % 3600) // 60)
            seconds = int(elapsed % 60)
            time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            stats_table.add_row("⏱️ Elapsed:", f"[dim]{time_str}[/dim]")

            # ETA calculation
            if self._stats["transferred_bytes"] > 0 and self._stats["total_bytes"] > 0:
                remaining = self._stats["total_bytes"] - self._stats["transferred_bytes"]
                if remaining > 0 and current_speed > 0:
                    eta_seconds = remaining / current_speed
                    eta_hours = int(eta_seconds // 3600)
                    eta_minutes = int((eta_seconds % 3600) // 60)
                    eta_str = f"{eta_hours:02d}:{eta_minutes:02d}"
                    stats_table.add_row("⏳ ETA:", f"[dim]{eta_str}[/dim]")

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
        """Enhanced footer with system info."""
        self._update_footer_display()

    def _update_footer_display(self):
        """Update footer with detailed status."""
        with self._lock:
            # Create a mini table for footer info
            footer_table = Table.grid(padding=(0, 3))
            footer_table.add_column(justify="left")
            footer_table.add_column(justify="center")
            footer_table.add_column(justify="right")

            # Left: Current operation
            left_text = "[dim]Status:[/] [green]Running[/]"

            # Center: Progress summary
            if self._stats["total_torrents"] > 0:
                progress_pct = (self._stats["completed_transfers"] / self._stats["total_torrents"]) * 100
                center_text = f"[cyan]{self._stats['completed_transfers']}/{self._stats['total_torrents']} torrents[/] ([yellow]{progress_pct:.0f}%[/])"
            else:
                center_text = "[dim]Waiting for torrents...[/]"

            # Right: Connection info
            right_text = f"[dim]SSH Pools: Active[/]"

            footer_table.add_row(left_text, center_text, right_text)

            self.layout["footer"].update(
                Panel(footer_table, border_style="dim", style="on #0a0e27")
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
            self._stats_thread.join()
            self.main_progress.stop()
            self.files_progress.stop()
            self._live.stop()

    def _stats_updater(self):
        """Background thread to update stats and displays."""
        while not self._stats_thread_stop.wait(1.0):
            self._update_stats_display()
            self._update_current_torrents()
            self._update_footer_display()

    # ===== Public API (keeping existing methods) =====

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
                               total_size: float, transfer_multiplier: int = 1):
        with self._lock:
            self._torrents[torrent_hash] = {
                "name": torrent_name,
                "size": total_size * transfer_multiplier,
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
            description=f"[yellow]⚡ Current: {display_name}",
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
                task_id = self.active_file_tasks.pop(oldest_file)
                self.files_progress.remove_task(task_id)
            except StopIteration:
                pass

        # Add new file with icon
        file_name = file_path.split('/')[-1]
        display_name = file_name[:45] + "..." if len(file_name) > 45 else file_name
        task_id = self.files_progress.add_task(
            f"📄 {display_name}",
            total=file_size
        )
        self.active_file_tasks[file_path] = task_id

    def update_file_progress(self, file_path: str, bytes_transferred: int):
        if file_path in self.active_file_tasks:
            self.files_progress.update(
                self.active_file_tasks[file_path],
                advance=bytes_transferred
            )

    def complete_file_transfer(self, file_path: str):
        if file_path in self.active_file_tasks:
            task_id = self.active_file_tasks.pop(file_path)
            self.files_progress.remove_task(task_id)

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
        self.main_progress.update(self.current_torrent_task, visible=False)

    def update_header(self, text: str):
        self.header_text = Text(f"🚀 Torrent Mover v{self.version} - {text}", justify="center", style="bold magenta")
        self.layout["header"].update(Panel(self.header_text, border_style="magenta", style="on #1a1a2e"))

    def update_footer(self, text: str):
        # Footer is now managed by _update_footer_display
        pass

    def log(self, message: str):
        self.console.log(message)

    def display_stats(self, stats: Dict[str, Any]) -> None:
        """Displays final statistics."""
        self.console.log("--- Final Statistics ---")
        self.console.log(f"Total Bytes Transferred: {self._stats['transferred_bytes'] / (1024**3):.2f} GB")
        self.console.log(f"Peak Speed: {self._stats['peak_speed'] / (1024**2):.2f} MB/s")
        self.console.log(f"Successful Transfers: {self._stats['completed_transfers']}")
        self.console.log(f"Failed Transfers: {self._stats['failed_transfers']}")
        self.console.log(f"Total Duration: {time.time() - self._stats['start_time']:.2f} seconds")