import pytest
from unittest.mock import MagicMock, patch, call
import configparser
import argparse
from pathlib import Path

# --- Mocks ---
# Import our mock classes from the harness
from tests.mocks.mock_qbittorrent import MockQBittorrentClient, MockTorrent
from tests.mocks.mock_ssh import MockSSHConnectionPool

# --- Classes and Functions to Test ---
# We are testing the standalone functions from torrent_mover.py
from torrent_mover import (
    TorrentMover,
    _post_transfer_actions,
    _pre_transfer_setup
)

# --- Fixtures ---

@pytest.fixture
def mock_config():
    """Provides a basic mock ConfigParser object."""
    config = configparser.ConfigParser()
    config['SETTINGS'] = {
        'transfer_mode': 'rsync',
        'source_server_section': 'SOURCE_SERVER',
        'recheck_stuck_timeout': '60',
        'recheck_stopped_timeout': '15',
    }
    config['DESTINATION_PATHS'] = {
        'destination_path': '/remote/downloads',
        'remote_destination_path': '/remote/downloads-docker'
    }
    config['DESTINATION_CLIENT'] = {
        'add_torrents_paused': 'true',
        'start_torrents_after_recheck': 'true',
    }
    config['SOURCE_CLIENT'] = {
        'delete_after_transfer': 'true'
    }
    config['SOURCE_SERVER'] = {} # Needed for pool
    return config

@pytest.fixture
def mock_args():
    """Provides a basic mock argparse.Namespace."""
    return argparse.Namespace(
        dry_run=False,
        test_run=False,
        parallel_jobs=1
    )

@pytest.fixture
def mock_ui():
    """Provides a mock BaseUIManager."""
    ui = MagicMock()
    ui.log = MagicMock()
    ui.complete_torrent_transfer = MagicMock()
    return ui

@pytest.fixture
def mock_ssh():
    """Provides a mock SSHConnectionPool."""
    return MagicMock(spec=MockSSHConnectionPool)

@pytest.fixture
def mock_dependencies(mock_config, mock_args, mock_ui):
    """A bundle of common mock dependencies for the worker."""
    return {
        "source_qbit": MagicMock(),
        "destination_qbit": MagicMock(),
        "config": mock_config,
        "tracker_rules": {},
        "ui": mock_ui,
        "file_tracker": MagicMock(),
        "ssh_connection_pools": {
            'SOURCE_SERVER': MagicMock(spec=MockSSHConnectionPool)
        },
        "checkpoint": MagicMock(),
        "args": mock_args,
        "log_transfer": MagicMock(),
        "_update_transfer_progress": MagicMock()
    }

# --- Tests ---

@patch('torrent_mover.transfer_torrent')
@patch('torrent_mover.logging')
def test_transfer_worker_skips_failed_torrent(mock_logging, mock_transfer_torrent, mock_dependencies, mock_ui):
    """
    Tests the "log and skip" logic from Directive #3.
    Verifies that if one torrent fails, the worker logs the error
    and moves on, without crashing.
    """
    # --- Setup ---
    # Create two mock torrents
    torrent1 = MockTorrent(name="Torrent 1", hash_="hash1", content_path="/src/t1", save_path="/src")
    torrent1.hash = torrent1.hash_
    torrent2 = MockTorrent(name="Torrent 2", hash_="hash2", content_path="/src/t2", save_path="/src")
    torrent2.hash = torrent2.hash_

    # Configure the mock 'transfer_torrent' function
    # The first call (for torrent1) will raise an Exception
    # The second call (for torrent2) will succeed
    mock_transfer_torrent.side_effect = [
        Exception("Mocked rsync error"),
        ("success", "OK")
    ]

    # Create a TorrentMover instance (needed for the worker method)
    # Replicate the ConfigManager structure for the test
    mock_config_manager = MagicMock()
    mock_config_manager.config = mock_dependencies["config"]
    mover = TorrentMover(args=mock_dependencies["args"], config_manager=mock_config_manager, script_dir=Path("."))

    # Manually set the dependencies on the mover instance, as 'run' would normally do
    mover.source_qbit = mock_dependencies["source_qbit"]
    mover.destination_qbit = mock_dependencies["destination_qbit"]
    mover.tracker_rules = mock_dependencies["tracker_rules"]
    mover.ui = mock_dependencies["ui"]
    mover.file_tracker = mock_dependencies["file_tracker"]
    mover.ssh_connection_pools = mock_dependencies["ssh_connection_pools"]
    mover.checkpoint = mock_dependencies["checkpoint"]

    # --- Execute ---
    # We test the _transfer_worker method, which contains the try/except logic
    # We call it twice, simulating a ThreadPoolExecutor
    mover._transfer_worker(torrent1, total_size=100)
    mover._transfer_worker(torrent2, total_size=200)

    # --- Assert ---
    # 1. Assert 'transfer_torrent' was called for both torrents
    assert mock_transfer_torrent.call_count == 2

    # 2. Assert the error was logged correctly for the FAILED torrent
    mock_logging.error.assert_called_once_with(
        "An exception was thrown for torrent 'Torrent 1': Mocked rsync error",
        exc_info=True
    )

    # 3. Assert the UI was updated correctly for the FAILED torrent
    mock_ui.complete_torrent_transfer.assert_called_once_with("hash1", success=False)

    # 4. Assert that the second torrent (which succeeded) did NOT log an error
    # We check that the mock was not called a second time
    assert mock_logging.error.call_count == 1
    # (Assert: The script does not crash) - This is proven by execution reaching this line

@patch('torrent_mover.wait_for_recheck_completion', return_value="SUCCESS")
def test_post_transfer_calls_recheck(mock_wait_recheck, mock_dependencies):
    """
    Tests the "Re-check Logic" from Directive #4.
    Verifies that _post_transfer_actions correctly calls torrents_recheck
    on the destination and torrents_delete on the source.
    """
    # --- Setup ---
    torrent = MockTorrent(name="Test", hash_="hash123", content_path="/src/test", save_path="/src")
    # Patch the mock object to have the 'hash' attribute that the real API uses
    torrent.hash = torrent.hash_
    mock_dest_qbit = mock_dependencies["destination_qbit"]
    mock_src_qbit = mock_dependencies["source_qbit"]

    # Mock destination client to return the torrent
    mock_dest_qbit.torrents_info.return_value = [torrent]

    # --- Execute ---
    success, msg = _post_transfer_actions(
        torrent=torrent,
        source_qbit=mock_src_qbit,
        destination_qbit=mock_dest_qbit,
        config=mock_dependencies["config"],
        tracker_rules=mock_dependencies["tracker_rules"],
        ssh_connection_pools=mock_dependencies["ssh_connection_pools"],
        dest_content_path="/remote/downloads/test",
        destination_save_path="/remote/downloads-docker",
        transfer_executed=True,
        dry_run=False,
        test_run=False,
        file_tracker=mock_dependencies["file_tracker"],
        transfer_mode="rsync",
        all_files=[], # Not needed for this test
        ui=mock_dependencies["ui"],
        log_transfer=mock_dependencies["log_transfer"],
        _update_transfer_progress=mock_dependencies["_update_transfer_progress"]
    )

    # --- Assert ---
    assert success is True

    # 1. Assert Re-check was called on DESTINATION
    mock_dest_qbit.torrents_recheck.assert_called_once_with(torrent_hashes="hash123")

    # 2. Assert Re-check Wait was called
    mock_wait_recheck.assert_called_once()

    # 3. Assert Delete was called on SOURCE
    mock_src_qbit.torrents_delete.assert_called_once_with(torrent_hashes="hash123", delete_files=True)

@patch('torrent_mover.batch_get_remote_sizes', return_value={})
def test_pre_transfer_path_mapping(mock_batch_get_sizes, mock_config, mock_args, mock_ssh):
    """
    Tests the "File Path Mapping" logic from Directive #4.
    Verifies that _pre_transfer_setup correctly calculates the source
    and destination paths, especially for a single-file torrent inside a folder.
    """
    # --- Setup ---
    # This torrent is a single file inside its own folder
    torrent = MockTorrent(
        name="My.Movie.2023",
        hash_="hash123",
        content_path="/data/My.Movie.2023/My.Movie.2023.mkv", # Path to the file
        save_path="/data/" # Root save path
    )

    mock_ssh_pools = {'SOURCE_SERVER': mock_ssh}

    # --- Execute ---
    status, msg, source_path, dest_path, save_path = _pre_transfer_setup(
        torrent=torrent,
        total_size=1000,
        config=mock_config,
        ssh_connection_pools=mock_ssh_pools,
        args=mock_args,
        transfer_mode='rsync' # This is passed for clarity, but config has it
    )

    # --- Assert ---
    assert status == "not_exists" # Based on mock_batch_get_sizes returning {}

    # 1. Assert Source Path is the *folder*, not the file
    assert source_path == "/data/My.Movie.2023"

    # 2. Assert Dest Path is the destination_path + content_name
    assert dest_path == "/remote/downloads/My.Movie.2023"

    # 3. Assert Save Path is the remote_destination_path (from config)
    assert save_path == "/remote/downloads-docker"
