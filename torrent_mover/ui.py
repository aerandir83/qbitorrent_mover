# torrent_mover/ui.py

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
)
from rich.table import Table
from rich.text import Text
import threading
from collections import OrderedDict

class UIManager:
    """A class to manage the Rich UI for the torrent mover script."""

    def __init__(self):
        self.console = Console()
        self._lock = threading.RLock()
        self._live = None
        self._torrents_data = OrderedDict()

        # --- UI Components ---

        # 1. Header
        self.header_text = Text("Initializing...", justify="center")
        self.header_panel = Panel(self.header_text, title="[bold magenta]Torrent Mover v1.5.1[/bold magenta]", border_style="magenta")

        # 2. Run Progress (counts and overall size)
        self.analysis_progress = Progress(TextColumn("[cyan]Analyzed"), BarColumn(), MofNCompleteColumn())
        self.analysis_task = self.analysis_progress.add_task("Torrents", total=0, visible=False)

        self.transfer_progress = Progress(TextColumn("[blue]Completed"), BarColumn(), MofNCompleteColumn())
        self.transfer_task = self.transfer_progress.add_task("Torrents", total=0, visible=False)

        self.overall_progress = Progress(TextColumn("[green]Overall"), BarColumn(), TextColumn("[progress.percentage]{task.percentage:>3.0f}%"), TotalFileSizeColumn(), TransferSpeedColumn(), TimeRemainingColumn())
        self.overall_task = self.overall_progress.add_task("Total", total=0, visible=False)

        run_progress_group = Group(self.analysis_progress, self.transfer_progress, self.overall_progress)
        self.run_progress_panel = Panel(run_progress_group, title="[bold]Run Progress[/bold]", border_style="green")

        # 3. Torrents Table
        self.torrents_table_panel = Panel(self._build_torrents_table(), title="[bold]Transfer Queue[/bold]", border_style="cyan")

        # 4. Footer
        self.footer_text = Text("Waiting to start...", justify="center")
        self.footer_panel = Panel(self.footer_text, border_style="dim")

        # --- Main Layout ---
        self.layout = Table.grid(expand=True)
        self.layout.add_row(self.header_panel)
        self.layout.add_row(self.run_progress_panel)
        self.layout.add_row(self.torrents_table_panel)
        self.layout.add_row(self.footer_panel)

    def _build_torrents_table(self):
        """Builds a new, empty torrents table with the correct columns."""
        table = Table(show_header=True, header_style="bold cyan", border_style="dim", expand=True)
        table.add_column("Torrent Name", style="cyan", no_wrap=True, min_width=20)
        table.add_column("Size", style="magenta", width=12, justify="right")
        table.add_column("Progress", width=75) # Widen for the unified progress bar
        return table

    def __enter__(self):
        self._live = Live(self.layout, console=self.console, screen=True, redirect_stderr=False, refresh_per_second=10)
        self._live.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live:
            for data in self._torrents_data.values():
                if isinstance(data.get("progress_obj"), Progress): data["progress_obj"].stop()
                if isinstance(data.get("file_progress_obj"), Progress): data["file_progress_obj"].stop()
            self._live.stop()

    def log(self, message):
        """Logs a message to the Rich console, appearing above the live display."""
        self.console.log(message)

    def set_analysis_total(self, total):
        self.analysis_progress.update(self.analysis_task, total=total, visible=True)
        self.transfer_progress.update(self.transfer_task, total=total, visible=True)

    def advance_analysis_progress(self):
        self.analysis_progress.update(self.analysis_task, advance=1)

    def advance_transfer_progress(self):
        self.transfer_progress.update(self.transfer_task, advance=1)

    def set_overall_total(self, total):
        self.overall_progress.update(self.overall_task, total=total, visible=True)

    def advance_overall_progress(self, advance):
        self.overall_progress.update(self.overall_task, advance=advance)

    def update_header(self, text):
        self.header_text.plain = text

    def update_footer(self, text):
        self.footer_text.plain = text

    def add_torrent_to_plan(self, torrent_name, torrent_hash, size_str):
        """Adds a torrent to the UI table with 'Queued' status."""
        with self._lock:
            if torrent_hash in self._torrents_data:
                return
            self._torrents_data[torrent_hash] = {
                "name": torrent_name,
                "size": size_str,
                "status_text": "[dim]Queued[/dim]",
                "progress_obj": None,
                "file_progress_obj": None,
            }
            self._update_torrents_table()

    def update_torrent_status_text(self, torrent_hash, status, color="yellow"):
        """Updates the status text of a specific torrent, used for non-transfer states."""
        with self._lock:
            if torrent_hash in self._torrents_data:
                self._torrents_data[torrent_hash].update({
                    "status_text": f"[{color}]{status}[/{color}]",
                    "progress_obj": None,
                    "file_progress_obj": None,
                })
                self._update_torrents_table()

    def start_torrent_transfer(self, torrent_hash, total_size, total_files):
        """Creates and assigns a Rich Progress object for a specific torrent transfer."""
        with self._lock:
            if torrent_hash in self._torrents_data:
                # A single, unified progress bar that contains all the necessary columns
                unified_progress = Progress(
                    TextColumn("[bold blue]Transferring[/bold blue]"),
                    BarColumn(bar_width=None),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    "•",
                    TransferSpeedColumn(),
                    "•",
                    TimeRemainingColumn(),
                    "•",
                    # Custom text column to show file count from the task's 'fields'
                    TextColumn("[cyan]{task.fields[files]}[/cyan]"),
                    expand=True,
                )
                # Add a single task for byte progress, and store file counts in its fields
                task_id = unified_progress.add_task(
                    "bytes",
                    total=total_size,
                    files=f"0/{total_files} Files" # Initial value for our custom column
                )

                self._torrents_data[torrent_hash].update({
                    "progress_obj": unified_progress,
                    "byte_task_id": task_id,
                    "total_files": total_files,
                    "files_completed": 0,
                    "file_progress_obj": None, # Ensure old attribute is cleared
                    "file_task_id": None,      # Ensure old attribute is cleared
                })
                self._update_torrents_table()

    def update_torrent_byte_progress(self, torrent_hash, advance):
        """Updates the byte progress bar for a specific torrent."""
        with self._lock:
            if torrent_hash in self._torrents_data and self._torrents_data[torrent_hash].get("progress_obj"):
                data = self._torrents_data[torrent_hash]
                data["progress_obj"].update(data["byte_task_id"], advance=advance)

    def advance_torrent_file_progress(self, torrent_hash):
        """Advances the file count for a specific torrent."""
        with self._lock:
            if torrent_hash in self._torrents_data and self._torrents_data[torrent_hash].get("progress_obj"):
                data = self._torrents_data[torrent_hash]
                data["files_completed"] += 1
                # Update the custom 'files' field in the task
                new_files_text = f"{data['files_completed']}/{data['total_files']} Files"
                data["progress_obj"].update(data["byte_task_id"], files=new_files_text)

    def stop_torrent_transfer(self, torrent_hash, success=True):
        """Stops the progress bar for a torrent and sets its final status."""
        with self._lock:
            if torrent_hash in self._torrents_data:
                data = self._torrents_data[torrent_hash]
                if data.get("progress_obj"):
                    task = data["progress_obj"].tasks[data["byte_task_id"]]
                    data["progress_obj"].update(data["byte_task_id"], completed=task.total)
                    data["progress_obj"].stop()

                if success:
                    self.update_torrent_status_text(torrent_hash, "Completed", color="green")
                else:
                    self.update_torrent_status_text(torrent_hash, "Failed", color="bold red")

    def _update_torrents_table(self):
        """Rebuilds the torrents table from scratch to reflect the current state."""
        new_table = self._build_torrents_table()

        for data in self._torrents_data.values():
            progress_renderable = data.get("status_text", "")

            if data.get("progress_obj"):
                progress_renderable = data["progress_obj"]

            new_table.add_row(
                Text(data["name"], overflow="ellipsis"),
                data["size"],
                progress_renderable,
            )

        self.torrents_table_panel.renderable = new_table
