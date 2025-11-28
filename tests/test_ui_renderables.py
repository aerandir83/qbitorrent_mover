import pytest
from unittest.mock import MagicMock, patch
from rich.console import Console
from collections import deque
import ui

@pytest.fixture
def mock_ui_manager():
    manager = MagicMock(spec=ui.UIManagerV2)
    manager._lock = MagicMock()
    manager._lock.__enter__ = MagicMock()
    manager._lock.__exit__ = MagicMock()
    manager._stats = {
        "start_time": 0,
        "current_dl_speed": 20 * 1024 * 1024, # 20.0 MB/s (Raw)
        "current_ul_speed": 0.0,
        "smoothed_dl_speed": 10 * 1024 * 1024, # 10.0 MB/s (Smoothed)
        "transferred_bytes": 0,
        "total_bytes": 100,
        "peak_speed": 2000.0,
        "active_transfers": 0,
        "completed_transfers": 0,
        "failed_transfers": 0,
    }
    manager._dl_speed_history = deque([10.0] * 60)
    manager._ul_speed_history = deque()
    manager._torrents = {}
    manager._layout_config = {"recent_completions": 0}
    manager._recent_completions = []
    manager.transfer_mode = "rsync"

    # Old attribute that ActivityPanel currently uses
    manager._speed_history_data = [999.0] * 10

    return manager

def test_stats_panel_uses_smoothed_speed(mock_ui_manager):
    panel = ui._StatsPanel(mock_ui_manager)
    console = Console(file=open("/dev/null", "w"))

    # Render
    result = list(panel.__rich_console__(console, None))

    # Capture output
    with console.capture() as capture:
        console.print(result[0])
    output = capture.get()

    # We expect "10.0" (smoothed) in the output.
    # Currently code uses current_dl_speed (20.0), so this assertion will FAIL before the fix.
    assert "10.0" in output, f"Expected smoothed speed (10.0) in output, got: {output}"
    assert "20.0" not in output, f"Did not expect raw speed (20.0) in output, got: {output}"

def test_activity_panel_uses_internal_history(mock_ui_manager):
    panel = ui._ActivityPanel(mock_ui_manager)
    console = Console(file=open("/dev/null", "w"))
    options = MagicMock()
    options.max_width = 50

    # Render
    result = list(panel.__rich_console__(console, options))

    # Result[0] is Panel. Panel.renderable is TextSparkline.
    # Note: Rich Panel stores renderable in .renderable
    sparkline = result[0].renderable

    # Verify the data used
    # Currently code uses _speed_history_data ([999.0]*10)
    # We expect it to use _dl_speed_history ([10.0]*60)

    # Check if we are looking at the right object
    assert hasattr(sparkline, 'data')

    # This assertion will FAIL before the fix
    assert list(sparkline.data) == list(mock_ui_manager._dl_speed_history)
    assert list(sparkline.data) != mock_ui_manager._speed_history_data
