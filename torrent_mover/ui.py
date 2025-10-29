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

# --- Custom Renderable Classes ---

class _StatsPanel:
    """A renderable class for the Stats and Recent Completions panels."""
    def __init__(self, ui_manager: "UIManagerV2"):
        self.ui_manager = ui_manager

    def __rich_console__(self, console: Console, options: Any) -> RenderResult:
        with self.ui_manager._lock:
            stats = self.ui_manager._stats
            recent_completions = self.ui_manager._recent_completions
            elapsed = time.time() - stats["start_time"]

            # Use last calculated speeds
            current_dl_speed = stats.get("current_dl_speed", 0.0)
            current_ul_speed = stats.get("current_ul_speed", 0.0)
            avg_speed_hist = sum(self.ui_manager._dl_speed_history) + sum(self.ui_manager._ul_speed_history)
            avg_speed_hist /= (len(self.ui_manager._dl_speed_history) + len(self.ui_manager._ul_speed_history)) if (self.ui_manager._dl_speed_history or self.ui_manager._ul_speed_history) else 1

            # Create stats table
            stats_table = Table.grid(padding=(0, 2))
            stats_table.add_column(style="bold cyan", justify="right", no_wrap=True)
            stats_table.add_column()

            transferred_gb = stats['transferred_bytes'] / (1024**3)
            total_gb = stats['total_bytes'] / (1024**3)
            remaining_gb = max(0, total_gb - transferred_gb)

            stats_table.add_row("📊 Transferred:", f"[white]{transferred_gb:.2f} / {total_gb:.2f} GB[/white]")
            stats_table.add_row("⏳ Remaining:", f"[white]{remaining_gb:.2f} GB[/white]")
            stats_table.add_row("⚡ DL Speed:", f"[red]{current_dl_speed / (1024**2):.2f} MB/s[/red]")
            stats_table.add_row("⚡ UL Speed:", f"[yellow]{current_ul_speed / (1024**2):.2f} MB/s[/yellow]")
            stats_table.add_row("📈 Avg Speed:", f"[dim]{avg_speed_hist / (1024**2):.2f} MB/s[/dim]")
            stats_table.add_row("🔥 Peak Speed:", f"[dim]{stats['peak_speed'] / (1024**2):.2f} MB/s[/dim]")
            stats_table.add_row("", "") # Spacer
            stats_table.add_row("🔄 Active:", f"[white]{stats['active_transfers']}[/white]")
            stats_table.add_row("✅ Completed:", f"[white]{stats['completed_transfers']}[/white]")
            stats_table.add_row("❌ Failed:", f"[white]{stats['failed_transfers']}[/white]")

            total_files_overall = sum(t.get('total_files', 0) for t in self.ui_manager._torrents.values())
            completed_files_overall = sum(t.get('completed_files', 0) for t in self.ui_manager._torrents.values())
            stats_table.add_row("📂 Files:", f"[white]{completed_files_overall} / {total_files_overall}[/white]")
            stats_table.add_row("", "") # Spacer

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

            # Create recent completions table
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
    """A renderable class for the Active Torrents list."""
    def __init__(self, ui_manager: "UIManagerV2"):
        self.ui_manager = ui_manager

    def __rich_console__(self, console: Console, options: Any) -> RenderResult:
        with self.ui_manager._lock:
            torrents = self.ui_manager._torrents
            stats = self.ui_manager._stats

            table = Table.grid(padding=(0, 1), expand=True)
            table.add_column(style="bold", no_wrap=True, width=4) # Progress %
            table.add_column() # Name & File List

            active_count = 0
            for hash_, torrent in torrents.items():
                if torrent["status"] == "transferring":
                    active_count += 1
                    name = torrent["name"]
                    display_name = name[:40] + "..." if len(name) > 43 else name
                    progress = torrent["transferred"] / torrent["size"] * 100 if torrent["size"] > 0 else 0

                    # Build file list
                    file_lines = []
                    files = torrent.get("files", {})

                    # Sort files: active, failed, queued, completed
                    def sort_key(item):
                        status = item[1].get("status", "queued")
                        if status in ["downloading", "uploading"]: return 0
                        if status == "failed": return 1
                        if status == "queued": return 2
                        return 3 # completed

                    sorted_files = sorted(files.items(), key=sort_key)

                    for file_path, file_info in sorted_files[:5]: # Limit to 5 files
                        file_name = file_path.split('/')[-1]
                        file_name = file_name[:35] + "..." if len(file_name) > 38 else file_name
                        status = file_info.get("status", "queued")

                        if status == "downloading":
                            file_lines.append(f" [red]⇩ {file_name}[/red]")
                        elif status == "uploading":
                            file_lines.append(f" [yellow]⇧ {file_name}[/yellow]")
                        elif status == "failed":
                            file_lines.append(f" [bold red]✖ {file_name}[/bold red]")
                        elif status == "completed":
                            file_lines.append(f" [dim]✓ {file_name}[/dim]")
                        else: # queued
                            file_lines.append(f" [dim]· {file_name}[/dim]")

                    if len(files) > 5:
                        file_lines.append(f" [dim]... and {len(files) - 5} more.[/dim]")

                    files_str = "\n".join(file_lines)
                    completed_files = torrent.get('completed_files', 0)
                    total_files = torrent.get('total_files', 0)

                    # Main torrent entry
                    table.add_row(
                        f"{progress:>3.0f}%",
                        f"[bold cyan]{display_name}[/bold cyan] [dim]({completed_files}/{total_files} files)[/dim]\n{files_str}"
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

        # Data structures for file-level tracking
        self._file_lists: Dict[str, List[str]] = {} # torrent_hash -> [file_name_1, file_name_2, ...]
        self._file_status: Dict[str, Dict[str, str]] = {} # torrent_hash -> {file_name -> "queued" | "downloading" | "uploading" | "completed"}

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


        # Create layout with better proportions
        self.layout = Layout()
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="body", ratio=1),
            Layout(name="footer", size=7)
        )

        # Split body into two columns
        self.layout["body"].split_row(
            Layout(name="left", ratio=1),  # Active Torrents
            Layout(name="right", ratio=1) # Stats
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
        """Setup progress bars with better formatting."""
        self.main_progress = Progress(
            TextColumn("[bold]{task.description}", justify="left"),
            BarColumn(bar_width=None, complete_style="green", finished_style="bold green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            "•",
            DownloadColumn(binary_units=True),
            "•",
            TextColumn("DL:[green]{task.fields.get('dl_speed', '0.00 MB/s'):>10}[/]"),
            "•",
            TextColumn("UL:[yellow]{task.fields.get('ul_speed', '0.00 MB/s'):>10}[/]"),
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
            fields={}
        )

        # Combine into left panel
        left_group = Group(
            Panel(
                self.main_progress,
                title="[bold green]📈 Transfer Progress",
                border_style="dim",
                style="on #16213e"
            ),
            _ActiveTorrentsPanel(self) # Add the new renderable panel
        )
        self.layout["left"].update(left_group)

    def _setup_stats_panel(self):
        """Setup stats panel using the renderable class."""
        self.layout["right"].update(_StatsPanel(self))

    def _setup_footer(self):
        """Initial setup for the log panel footer."""
        self.layout["footer"].update(_LogPanel(self))

    def __enter__(self):
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
        if self._live:
            self._stats_thread_stop.set()
            if self._stats_thread.is_alive():
                self._stats_thread.join(timeout=2.0)
            self.main_progress.stop()
            self._live.stop()

    def set_final_status(self, message: str):
        """
        Sets a final, persistent status message in the footer.
        """
        with self._lock:
            self._current_status = message

    def _stats_updater(self):
        """Background thread to update stats and progress bars."""
        while not self._stats_thread_stop.wait(1.0):
            with self._lock:
                # --- Calculate Speeds ---
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

                # --- UPDATE THREAD-SAFE PROGRESS BAR ---
                dl_speed_str = f"{current_dl_speed / (1024**2):.2f} MB/s"
                ul_speed_str = f"{current_ul_speed / (1024**2):.2f} MB/s"

                try:
                    self.main_progress.update(
                        self.overall_task,
                        fields={
                            'dl_speed': dl_speed_str,
                            'ul_speed': ul_speed_str
                        }
                    )
                except Exception as e:
                    logging.debug(f"Could not update overall task speeds: {e}")

    # ===== Public API (keeping existing methods) =====

    def set_transfer_mode(self, mode: str):
        with self._lock:
            self.transfer_mode = mode
            self._setup_header()

    def set_analysis_total(self, total: int):
        with self._lock:
            self._stats["total_torrents"] = total
            self.main_progress.update(self.analysis_task, total=total, visible=True)

    def advance_analysis(self):
        self.main_progress.update(self.analysis_task, advance=1)

    def complete_analysis(self):
        with self._lock:
            self._analysis_complete = True
            try:
                self.main_progress.update(self.analysis_task, visible=False)
            except Exception as e:
                logging.error(f"Error hiding analysis task: {e}")

    def set_overall_total(self, total_bytes: float):
        with self._lock:
            self._stats["total_bytes"] = total_bytes
            self.main_progress.update(self.overall_task, total=total_bytes, visible=True)

    def start_torrent_transfer(self, torrent_hash: str, torrent_name: str, total_size: float, all_files: List[str], transfer_multiplier: int = 1):
        with self._lock:
            total_files = len(all_files)
            self._torrents[torrent_hash] = {
                "name": torrent_name,
                "size": total_size * transfer_multiplier,
                "total_files": total_files,
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
        with self._lock:
            if torrent_hash in self._file_status and file_path in self._file_status[torrent_hash]:
                self._file_status[torrent_hash][file_path] = status

    def complete_file_transfer(self, torrent_hash: str, file_path: str):
        with self._lock:
            if torrent_hash in self._file_status and file_path in self._file_status[torrent_hash]:
                if self._file_status[torrent_hash][file_path] != "completed":
                    self._file_status[torrent_hash][file_path] = "completed"
                    if torrent_hash in self._torrents:
                        if "completed_files" not in self._torrents[torrent_hash]:
                            self._torrents[torrent_hash]["completed_files"] = 0
                        self._torrents[torrent_hash]["completed_files"] += 1

    def fail_file_transfer(self, torrent_hash: str, file_path: str):
        with self._lock:
            if torrent_hash in self._file_status and file_path in self._file_status[torrent_hash]:
                self._file_status[torrent_hash][file_path] = "failed"

    def complete_torrent_transfer(self, torrent_hash: str, success: bool = True):
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
        with self._lock:
            timestamp = time.strftime("%H:%M:%S")
            self._log_buffer.append(Text.from_markup(f"[{timestamp}] {message}"))

    def display_stats(self, stats: Dict[str, Any]) -> None:
        self.console.print("--- Final Statistics ---", style="bold green")
        self.console.print(f"Total Bytes Transferred: {self._stats['transferred_bytes'] / (1024**3):.2f} GB")
        self.console.print(f"Peak Speed: {self._stats['peak_speed'] / (1024**2):.2f} MB/s")
        self.console.print(f"Successful Transfers: {self._stats['completed_transfers']}")
        self.console.print(f"Failed Transfers: {self._stats['failed_transfers']}")
        self.console.print(f"Total Duration: {time.time() - self._stats['start_time']:.2f} seconds")
