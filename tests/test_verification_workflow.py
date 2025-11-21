import pytest
from unittest.mock import MagicMock, patch
import configparser
import time
from torrent_mover import _post_transfer_actions
from qbittorrent_manager import wait_for_recheck_completion
from tests.mocks.mock_qbittorrent import MockTorrent
from tests.mocks.mock_ssh import MockSSHConnectionPool

# --- Fixtures ---
@pytest.fixture
def mock_config():
    config = configparser.ConfigParser()
    config['SETTINGS'] = {
        'transfer_mode': 'rsync',
        'manual_review_category': 'review-failed',
        'source_server_section': 'SOURCE_SERVER',
        'source_client_section': 'SOURCE_CLIENT',
        'destination_client_section': 'DESTINATION_CLIENT',
    }
    config['DESTINATION_PATHS'] = {
        'destination_path': '/dest',
        'remote_destination_path': '/dest'
    }
    config['DESTINATION_CLIENT'] = {
        'add_torrents_paused': 'true',
        'start_torrents_after_recheck': 'true',
    }
    config['SOURCE_CLIENT'] = {
        'delete_after_transfer': 'true'
    }
    config['SOURCE_SERVER'] = {}
    config['DESTINATION_SERVER'] = {}
    return config

@pytest.fixture
def mock_ui():
    ui = MagicMock()
    ui.log = MagicMock()
    ui.pet_watchdog = MagicMock()
    return ui

# --- Test qbittorrent_manager.py logic ---

@patch('qbittorrent_manager.time')
def test_wait_for_recheck_returns_failed_final_review(mock_time, mock_ui):
    """
    Verifies that wait_for_recheck_completion returns 'FAILED_FINAL_REVIEW'
    when the torrent is in a bad state and progress < 100% after timeout.
    """
    client = MagicMock()

    # Setup the sequence of events:
    # 1. Call torrents_info -> returns torrent in "checking" state
    # 2. Call torrents_info -> returns torrent in "error" state (progress 90%)
    # 3. Call torrents_info -> returns torrent in "error" state (progress 90%) - confirm timeout

    torrent_checking = MockTorrent(name="T", hash_="h", content_path="/", save_path="/", state="checkingDL", progress=0.5)
    torrent_error = MockTorrent(name="T", hash_="h", content_path="/", save_path="/", state="error", progress=0.9)

    client.torrents_info.side_effect = [
        [torrent_checking],
        [torrent_error],
        [torrent_error],
        [torrent_error], # Extra calls if loop continues
        [torrent_error]
    ]

    # We need to control time to trigger the timeout
    # Start time = 1000
    # Loop logic uses time.time()

    # initial last_progress_increase_time call
    # loop 1: state checking. time = 1000.
    # loop 2: state error. stopped_state_detected_time = 1001.
    # loop 3: state error. stopped_state_detected_time is set. elapsed = 20.

    mock_time.time.side_effect = [1000, 1000, 1001, 1022, 1022]
    mock_time.sleep = MagicMock()

    result = wait_for_recheck_completion(
        client=client,
        torrent_hash="h",
        ui=mock_ui,
        recheck_stuck_timeout=60,
        recheck_stopped_timeout=15,
        dry_run=False
    )

    assert result == "FAILED_FINAL_REVIEW"


# --- Test torrent_mover.py logic ---

@patch('torrent_mover.change_ownership', return_value=True)
def test_post_transfer_handles_failed_final_review(mock_chown, mock_config, mock_ui):
    """
    Verifies that _post_transfer_actions handles 'FAILED_FINAL_REVIEW' correctly:
    1. Does NOT delete source.
    2. Sets category to 'review-failed'.
    3. Pauses destination torrent.
    4. Returns False.
    """
    torrent = MockTorrent(name="Test", hash_="hash123", content_path="/src", save_path="/src")
    # Patch hash
    torrent.hash = torrent.hash_

    dest_client = MagicMock()
    dest_client.get_torrent_info.return_value = torrent
    dest_client.wait_for_recheck.return_value = "FAILED_FINAL_REVIEW"

    source_client = MagicMock()

    # Call function
    success, msg = _post_transfer_actions(
        torrent=torrent,
        source_client=source_client,
        destination_client=dest_client,
        config=mock_config,
        tracker_rules={},
        ssh_connection_pools={},
        dest_content_path="/dest",
        destination_save_path="/dest",
        transfer_executed=True,
        dry_run=False,
        test_run=False,
        file_tracker=MagicMock(),
        transfer_mode="rsync",
        all_files=[],
        ui=mock_ui,
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock()
    )

    # Assertions
    assert success is False
    assert "Verification failed" in msg

    # Check category set
    dest_client.set_category.assert_called_with(torrent_hash="hash123", category="review-failed")

    # Check pause
    dest_client.pause_torrent.assert_called_with(torrent_hash="hash123")

    # Check source NOT deleted
    source_client.delete_torrent.assert_not_called()
