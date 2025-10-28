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
import logging
import threading
from collections import OrderedDict, deque
from typing import Dict, Any, Optional, Deque, Tuple
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
        self._current_header_string_template: str = ""
        self._last_header_text_part: str = ""
        self._current_status: Optional[str] = None
        self._analysis_complete = False # Add this flag

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
            "last_bytes": 0, # Bytes at last speed check
            "last_speed_check": time.time(), # Time of last speed check
        }
        self._speed_history = deque(maxlen=60) # Store last 60 seconds of speed readings


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
        header_content = f"🚀 Torrent Mover v{self.version} {mode_str} - [green]Initializing...[/]" # Raw f-string
        self.layout["header"].update(
            Panel(
                Align.center(header_content), # Pass string inside Align.center
                title="[bold magenta]TORRENT MOVER[/]",
                border_style="dim",
                style="on #1a1a2e"
            )
        )
        # Store the latest raw string for update_header
        self._current_header_string_template = "🚀 Torrent Mover v{version} {mode_str} - {text}"
        self._last_header_text_part = "[green]Initializing...[/]"

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

        # Combine into left panel
        progress_group = Group(
            Panel(
                self.main_progress,
                title="[bold green]📈 Transfer Progress",
                border_style="dim",
                style="on #16213e"
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

            # --- START SPEED CALCULATION FIX ---
            current_speed = 0.0
            time_since_last = time.time() - self._stats["last_speed_check"]

            if time_since_last >= 1.0: # Calculate roughly every second
                bytes_since_last = self._stats["transferred_bytes"] - self._stats["last_bytes"]
                current_speed = bytes_since_last / time_since_last

                # Update trackers for next calculation
                self._stats["last_bytes"] = self._stats["transferred_bytes"]
                self._stats["last_speed_check"] = time.time()

                # Update the stored current speed
                self._stats["current_speed"] = current_speed
                self._speed_history.append(current_speed) # Add to history
            else:
                # Use the last calculated speed if less than a second passed
                current_speed = self._stats["current_speed"]

            # Calculate average speed over the history for ETA
            avg_speed_hist = sum(self._speed_history) / len(self._speed_history) if self._speed_history else 0.0
            # --- END SPEED CALCULATION FIX ---


            # Track peak speed (using instantaneous speed)
            if current_speed > self._stats["peak_speed"]:
                self._stats["peak_speed"] = current_speed

            # Create stats table
            stats_table = Table.grid(padding=(0, 2))
            stats_table.add_column(style="bold cyan", justify="right", no_wrap=True)
            stats_table.add_column()

            # Transfer stats
            transferred_gb = self._stats['transferred_bytes'] / (1024**3)
            total_gb = self._stats['total_bytes'] / (1024**3)
            remaining_gb = max(0, total_gb - transferred_gb)

            stats_table.add_row("📊 Transferred:", f"[white]{transferred_gb:.2f} / {total_gb:.2f} GB[/white]")
            stats_table.add_row("⏳ Remaining:", f"[white]{remaining_gb:.2f} GB[/white]")
            # Display the calculated current speed
            stats_table.add_row("⚡ Speed:", f"[white]{current_speed / (1024**2):.2f} MB/s[/white]")
            stats_table.add_row("📈 Avg Speed:", f"[dim]{avg_speed_hist / (1024**2):.2f} MB/s[/dim]") # Display historical average
            stats_table.add_row("🔥 Peak Speed:", f"[dim]{self._stats['peak_speed'] / (1024**2):.2f} MB/s[/dim]")

            stats_table.add_row("", "") # Spacer

            # Status stats (rest of the method remains similar...)
            stats_table.add_row("🔄 Active:", f"[white]{self._stats['active_transfers']}[/white]")
            stats_table.add_row("✅ Completed:", f"[white]{self._stats['completed_transfers']}[/white]")
            stats_table.add_row("❌ Failed:", f"[white]{self._stats['failed_transfers']}[/white]")

            total_files_overall = sum(t.get('total_files', 0) for t in self._torrents.values())
            completed_files_overall = sum(t.get('completed_files', 0) for t in self._torrents.values())
            stats_table.add_row("📂 Files:", f"[white]{completed_files_overall} / {total_files_overall}[/white]")

            stats_table.add_row("", "") # Spacer

            hours = int(elapsed // 3600)
            minutes = int((elapsed % 3600) // 60)
            seconds = int(elapsed % 60)
            time_str = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            stats_table.add_row("⏱️ Elapsed:", f"[dim]{time_str}[/dim]")

            # ETA calculation (Use avg_speed_hist for stability)
            if remaining_gb > 0 and avg_speed_hist > 0:
                eta_seconds = (remaining_gb * 1024**3) / avg_speed_hist
                eta_hours = int(eta_seconds // 3600)
                eta_minutes = int((eta_seconds % 3600) // 60)
                eta_str = f"{eta_hours:02d}:{eta_minutes:02d}"
                stats_table.add_row("⏳ ETA:", f"[cyan]{eta_str}[/]")

            # ... (rest of the method for recent completions)
            if self._recent_completions:
                # ... (recent completions table code remains the same) ...
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
        """Updates the footer with the latest log messages or a final status."""
        with self._lock:
            if self._current_status:
                display_content = Align.center(Text(self._current_status, style="bold green"))
            else:
                # Join the deque of Text objects with newlines
                display_content = Align.left(Text("\n").join(list(self._log_buffer)))

            self.layout["footer"].update(
                Panel(
                    display_content,
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
            refresh_per_second=10
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
                self._stats_thread.join(timeout=2.0) # <-- ADD TIMEOUT
            self.main_progress.stop()
            self._live.stop()

    def set_final_status(self, message: str):
        """
        Sets a final, persistent status message in the footer.
        """
        with self._lock:
            self._current_status = message
        # Manually refresh the footer one last time
        self._update_footer_display()

    def _stats_updater(self):
        """Background thread to update stats and displays."""
        while not self._stats_thread_stop.wait(1.0):
            with self._lock:
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

    def complete_analysis(self):
        """Mark analysis as complete and hide the analysis progress bar."""
        with self._lock:
            self._analysis_complete = True
            # Update the task directly to hide it
            try:
                self.main_progress.update(self.analysis_task, visible=False)
            except Exception as e:
                # Log if the task doesn't exist or another error occurs
                logging.error(f"Error hiding analysis task: {e}")

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

    def update_torrent_progress(self, torrent_hash: str, bytes_transferred: float):
        with self._lock:
            if torrent_hash in self._torrents:
                self._torrents[torrent_hash]["transferred"] += bytes_transferred
                self._stats["transferred_bytes"] += bytes_transferred

            # Update progress bars
            self.main_progress.update(self.overall_task, advance=bytes_transferred)

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

    def log(self, message: str):
        """Adds a message to the on-screen log buffer."""
        with self._lock:
            # Format with a timestamp
            timestamp = time.strftime("%H:%M:%S")
            self._log_buffer.append(Text.from_markup(f"[{timestamp}] {message}"))

    def display_stats(self, stats: Dict[str, Any]) -> None:
        """Displays final statistics."""
        # This is less important now as stats are live
        self.console.print("--- Final Statistics ---", style="bold green")
        self.console.print(f"Total Bytes Transferred: {self._stats['transferred_bytes'] / (1024**3):.2f} GB")
        self.console.print(f"Peak Speed: {self._stats['peak_speed'] / (1024**2):.2f} MB/s")
        self.console.print(f"Successful Transfers: {self._stats['completed_transfers']}")
        self.console.print(f"Failed Transfers: {self._stats['failed_transfers']}")
        self.console.print(f"Total Duration: {time.time() - self._stats['start_time']:.2f} seconds")
