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
        table = Table.grid(expand=True)
        table.add_column("Details")
        return table

    def __enter__(self):
        self._live = Live(self.layout, console=self.console, screen=True, redirect_stderr=False, refresh_per_second=10)
        self._live.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._live:
            for data in self._torrents_data.values():
                if isinstance(data.get("byte_progress_obj"), Progress):
                    data["byte_progress_obj"].stop()
                if isinstance(data.get("file_progress_obj"), Progress):
                    data["file_progress_obj"].stop()
                if "files" in data:
                    for file_data in data["files"].values():
                        if isinstance(file_data.get("progress_obj"), Progress):
                            file_data["progress_obj"].stop()
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
        self.header_panel.renderable = Text(text, justify="center")

    def update_footer(self, text):
        self.footer_panel.renderable = Text(text, justify="center")

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
                "files": OrderedDict(),
            }
            self._update_torrents_table()

    def update_torrent_status_text(self, torrent_hash, status, color="yellow"):
        """Updates the status text of a specific torrent, used for non-transfer states."""
        with self._lock:
            if torrent_hash in self._torrents_data:
                self._torrents_data[torrent_hash].update({
                    "status_text": f"[{color}]{status}[/{color}]",
                    "progress_obj": None,
                })
                self._update_torrents_table()

    def start_torrent_transfer(self, torrent_hash, total_size, files_to_transfer, transfer_multiplier=1):
        with self._lock:
            if torrent_hash not in self._torrents_data:
                return

            byte_progress = Progress(
                TextColumn("[bold blue]Bytes[/bold blue]"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                "•",
                TotalFileSizeColumn(),
            )
            byte_task = byte_progress.add_task("Bytes", total=total_size * transfer_multiplier)

            file_progress = Progress(
                TextColumn("[bold blue]Files[/bold blue]"),
                BarColumn(),
                MofNCompleteColumn(),
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
            self._update_torrents_table()

    def update_torrent_byte_progress(self, torrent_hash, advance):
        with self._lock:
            data = self._torrents_data.get(torrent_hash)
            if data and data.get("byte_progress_obj"):
                data["byte_progress_obj"].update(data["byte_task_id"], advance=advance)

    def advance_torrent_file_progress(self, torrent_hash):
        with self._lock:
            data = self._torrents_data.get(torrent_hash)
            if data and data.get("file_progress_obj"):
                data["file_progress_obj"].update(data["file_task_id"], advance=1)

    def start_file_transfer(self, torrent_hash, file_path, file_size):
        with self._lock:
            data = self._torrents_data.get(torrent_hash)
            if not data or file_path not in data["files"]:
                return

            progress = Progress(
                TextColumn("[blue]{task.description}[/blue]"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                "•",
                TransferSpeedColumn(),
            )
            task_id = progress.add_task("Starting...", total=file_size)
            data["files"][file_path] = {
                "progress_obj": progress,
                "task_id": task_id,
                "status": "Starting..."
            }

    def update_file_status(self, torrent_hash, file_path, status):
        with self._lock:
            data = self._torrents_data.get(torrent_hash)
            if data and file_path in data["files"] and data["files"][file_path].get("progress_obj"):
                file_data = data["files"][file_path]
                file_data["progress_obj"].update(file_data["task_id"], description=status)
                file_data["status"] = status

    def update_file_progress(self, torrent_hash, file_path, advance):
        with self._lock:
            data = self._torrents_data.get(torrent_hash)
            if data and file_path in data["files"] and data["files"][file_path].get("progress_obj"):
                data["files"][file_path]["progress_obj"].update(data["files"][file_path]["task_id"], advance=advance)

    def stop_torrent_transfer(self, torrent_hash, success=True):
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

    def _update_torrents_table(self):
        """Rebuilds the torrents table from scratch to reflect the current state."""
        main_grid = self._build_torrents_table()

        for torrent_hash, data in self._torrents_data.items():
            # Main container for a single torrent's display
            torrent_grid = Table.grid(expand=True)
            # Row 1: Torrent Name and Size
            header_table = Table(show_header=False, box=None, padding=0, expand=True)
            header_table.add_column()
            header_table.add_column(justify="right")
            header_table.add_row(f"[bold cyan]{data['name']}[/bold cyan]", f"[magenta]{data['size']}[/magenta]")
            torrent_grid.add_row(header_table)

            # Row 2: Overall Torrent Progress Bar or Status Text
            if data.get("progress_obj"):
                torrent_grid.add_row(data["progress_obj"])
            elif data.get("status_text"):
                torrent_grid.add_row(Panel(Text.from_markup(data['status_text'], justify="center"), style="dim"))

            # Row 3: File details in a nested table
            if data["files"]:
                files_table = Table(show_header=True, header_style="bold blue", box=None, padding=(0, 1), expand=True)
                files_table.add_column("File", no_wrap=True)
                files_table.add_column("Progress", no_wrap=True)

                for file_path, file_data in data["files"].items():
                    file_name = file_path.split('/')[-1]
                    progress_renderable = file_data.get("progress_obj") or Text(file_data["status"], justify="center")
                    files_table.add_row(file_name, progress_renderable)

                torrent_grid.add_row(Panel(files_table, style="blue", border_style="dim"))

            main_grid.add_row(Panel(torrent_grid, border_style="cyan" if data.get("progress_obj") else "dim"))

        self.torrents_table_panel.renderable = main_grid
