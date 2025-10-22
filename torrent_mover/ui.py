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
    FileSizeColumn,
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
        self._job_progress_tasks = {}

        # --- UI Components ---

        # 1. Header
        self.header_text = Text("Initializing...", justify="center")
        self.header_panel = Panel(self.header_text, title="[bold magenta]Torrent Mover v1.4.0[/bold magenta]", border_style="magenta")

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
        self.torrents_table = Table(show_header=True, header_style="bold cyan", border_style="dim", expand=True)
        self.torrents_table.add_column("Torrent Name", style="cyan", no_wrap=True)
        self.torrents_table.add_column("Size", style="magenta", width=12, justify="right")
        self.torrents_table.add_column("Status", style="yellow", width=25)
        self.torrents_table_panel = Panel(self.torrents_table, title="[bold]Transfer Queue[/bold]", border_style="cyan")

        # 4. Active Transfers Panel
        self.job_progress = Progress(
            TextColumn("  {task.description}", justify="left"),
            BarColumn(finished_style="green"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            FileSizeColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn()
        )
        self.active_transfers_panel = Panel(self.job_progress, title="[bold]Active Transfers[/bold]", border_style="yellow", height=8)

        # 5. Footer
        self.footer_text = Text("Waiting to start...", justify="center")
        self.footer_panel = Panel(self.footer_text, border_style="dim")

        # --- Main Layout ---
        self.layout = Table.grid(expand=True)
        self.layout.add_row(self.header_panel)
        self.layout.add_row(self.run_progress_panel)
        self.layout.add_row(self.torrents_table_panel)
        self.layout.add_row(self.active_transfers_panel)
        self.layout.add_row(self.footer_panel)


    def __enter__(self):
        self._live = Live(self.layout, console=self.console, screen=True, redirect_stderr=False, refresh_per_second=5)
        self._live.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live:
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
                "status": "[dim]Queued[/dim]",
            }
            self._update_torrents_table()

    def update_torrent_status(self, torrent_hash, status, color="yellow"):
        """Updates the status text of a specific torrent."""
        with self._lock:
            if torrent_hash in self._torrents_data:
                self._torrents_data[torrent_hash]["status"] = f"[{color}]{status}[/{color}]"
                self._update_torrents_table()

    def start_torrent_transfer(self, torrent_hash, total_size):
        """Adds a torrent to the 'Active Transfers' panel."""
        with self._lock:
            if torrent_hash in self._torrents_data and torrent_hash not in self._job_progress_tasks:
                name = self._torrents_data[torrent_hash]["name"]
                task_id = self.job_progress.add_task(name, total=total_size)
                self._job_progress_tasks[torrent_hash] = task_id
                self.update_torrent_status(torrent_hash, "Transferring", color="bold blue")

    def update_torrent_progress(self, torrent_hash, advance):
        """Updates the progress bar for a specific torrent in the active panel."""
        with self._lock:
            if torrent_hash in self._job_progress_tasks:
                task_id = self._job_progress_tasks[torrent_hash]
                self.job_progress.update(task_id, advance=advance)

    def stop_torrent_transfer(self, torrent_hash, success=True):
        """Removes a torrent from the 'Active Transfers' panel."""
        with self._lock:
            if torrent_hash in self._job_progress_tasks:
                task_id = self._job_progress_tasks.pop(torrent_hash)
                # Mark as complete and stop the task to remove speed/ETA
                self.job_progress.update(task_id, completed=self.job_progress.tasks[task_id].total)
                self.job_progress.stop_task(task_id)
                # Hide it after a short delay so it doesn't disappear instantly
                self.job_progress.update(task_id, visible=False)

                if success:
                    self.update_torrent_status(torrent_hash, "Completed", color="green")
                else:
                    self.update_torrent_status(torrent_hash, "Failed", color="bold red")

    def _update_torrents_table(self):
        """Rebuilds the torrents table from scratch and swaps it into the panel."""

        # Create a new table with the same structure
        new_table = Table(show_header=True, header_style="bold cyan", border_style="dim", expand=True)
        new_table.add_column("Torrent Name", style="cyan", no_wrap=True)
        new_table.add_column("Size", style="magenta", width=12, justify="right")
        new_table.add_column("Status", style="yellow", width=25)

        # Populate the new table with the current data
        for data in self._torrents_data.values():
            new_table.add_row(
                Text(data["name"], overflow="ellipsis"),
                data["size"],
                data["status"],
            )

        # Atomically update the panel's renderable
        self.torrents_table_panel.renderable = new_table
