import pytest
from unittest.mock import MagicMock, patch
import configparser
import argparse
import os

# --- Mocks ---
from tests.mocks.mock_qbittorrent import MockTorrent
from tests.mocks.mock_ssh import MockSSHConnectionPool

# --- Functions to Test ---
from torrent_mover import (
    _pre_transfer_setup,
    _post_transfer_actions
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
    config['SOURCE_SERVER'] = {}
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
    return ui

@pytest.fixture
def mock_ssh():
    """Provides a mock SSHConnectionPool."""
    return MagicMock(spec=MockSSHConnectionPool)

# --- Tests ---

@patch('torrent_mover.batch_get_remote_sizes')
@patch('os.path.exists')
def test_pre_transfer_forces_remote_check(mock_exists, mock_batch_get_sizes, mock_config, mock_args, mock_ssh):
    """
    Test Step A: Modify _pre_transfer_setup to force remote size checks if DESTINATION_SERVER is configured.
    """
    # --- Setup ---
    # Add DESTINATION_SERVER to config
    mock_config['DESTINATION_SERVER'] = {
        'host': 'dest_host',
        'username': 'user',
        'password': 'password'
    }

    # Configure pool to return a valid connection
    # We need to configure the mock to return a tuple when used as a context manager
    mock_context = MagicMock()
    mock_context.__enter__.return_value = (MagicMock(), MagicMock())
    mock_context.__exit__.return_value = None
    mock_ssh.get_connection.return_value = mock_context

    mock_ssh_pools = {'DESTINATION_SERVER': mock_ssh}

    # Mock remote size check to return a size (simulating file exists)
    dest_path = "/remote/downloads/MyTorrent"
    mock_batch_get_sizes.return_value = {dest_path: 1000}

    # Mock local exists check (should NOT be called or ignored if remote check runs)
    mock_exists.return_value = False

    torrent = MockTorrent(
        name="MyTorrent",
        hash_="hash123",
        content_path="/data/MyTorrent",
        save_path="/data/"
    )

    # --- Execute ---
    status, msg, source_path, dest_path_out, save_path = _pre_transfer_setup(
        torrent=torrent,
        total_size=1000,
        config=mock_config,
        ssh_connection_pools=mock_ssh_pools,
        args=mock_args,
        transfer_mode='rsync' # Even with rsync (usually local), it should check remote because DESTINATION_SERVER exists
    )

    # --- Assert ---
    # 1. Verify remote check was performed
    mock_batch_get_sizes.assert_called_once()

    # 2. Verify status reflects the remote size (exists_same_size because 1000 == 1000)
    assert status == "exists_same_size"

    # 3. Verify local check was NOT the deciding factor (we mocked it to False)
    # If it relied on local check, status would be "not_exists" (or equivalent)


@patch('torrent_mover.batch_get_remote_sizes')
@patch('os.path.exists')
def test_pre_transfer_falls_back_to_local_check(mock_exists, mock_batch_get_sizes, mock_config, mock_args, mock_ssh):
    """
    Test fallback: If DESTINATION_SERVER is NOT configured, use local check.
    """
    # --- Setup ---
    # Ensure NO DESTINATION_SERVER in config (default fixture setup)
    if 'DESTINATION_SERVER' in mock_config:
        del mock_config['DESTINATION_SERVER']

    mock_ssh_pools = {} # No destination pool

    # Mock local exists check to True
    mock_exists.return_value = True
    # Mock isdir check for size calculation
    with patch('os.path.isdir', return_value=False), \
         patch('os.path.getsize', return_value=1000):

        torrent = MockTorrent(
            name="MyTorrent",
            hash_="hash123",
            content_path="/data/MyTorrent",
            save_path="/data/"
        )

        # --- Execute ---
        status, msg, source_path, dest_path_out, save_path = _pre_transfer_setup(
            torrent=torrent,
            total_size=1000,
            config=mock_config,
            ssh_connection_pools=mock_ssh_pools,
            args=mock_args,
            transfer_mode='rsync'
        )

        # --- Assert ---
        # 1. Verify remote check was NOT performed
        mock_batch_get_sizes.assert_not_called()

        # 2. Verify local check WAS performed
        mock_exists.assert_called()

        # 3. Verify status reflects local check
        assert status == "exists_same_size"

def test_post_transfer_uses_dynamic_source_section(mock_config, mock_ui, mock_ssh):
    """
    Test Step B: Modify _post_transfer_actions to dynamically retrieve correct source section.
    """
    # --- Setup ---
    # 1. Rename default source section
    del mock_config['SOURCE_CLIENT']
    mock_config['MY_CUSTOM_SOURCE'] = {
        'delete_after_transfer': 'true'
    }
    # 2. Update SETTINGS to point to new section
    mock_config['SETTINGS']['source_client_section'] = 'MY_CUSTOM_SOURCE'

    torrent = MockTorrent(name="Test", hash_="hash123", content_path="/src/test", save_path="/src")
    torrent.hash = torrent.hash_

    mock_src_client = MagicMock()
    mock_dest_client = MagicMock()
    mock_dest_client.get_torrent_info.return_value = torrent
    mock_dest_client.wait_for_recheck.return_value = "SUCCESS"

    mock_file_tracker = MagicMock()

    # --- Execute ---
    success, msg = _post_transfer_actions(
        torrent=torrent,
        source_client=mock_src_client,
        destination_client=mock_dest_client,
        config=mock_config,
        tracker_rules={},
        ssh_connection_pools={},
        dest_content_path="/remote/downloads/test",
        destination_save_path="/remote/downloads-docker",
        transfer_executed=True,
        dry_run=False,
        test_run=False,
        file_tracker=mock_file_tracker,
        transfer_mode="rsync",
        all_files=[],
        ui=mock_ui,
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock()
    )

    # --- Assert ---
    # 1. Verify delete_torrent was called
    # This implies it correctly found 'delete_after_transfer' = True in 'MY_CUSTOM_SOURCE'
    mock_src_client.delete_torrent.assert_called_once_with(torrent_hash="hash123", delete_files=True)

def test_post_transfer_respects_false_deletion_config(mock_config, mock_ui, mock_ssh):
    """
    Test Step B (Negative): Verify it respects 'false' setting in custom section.
    """
    # --- Setup ---
    del mock_config['SOURCE_CLIENT']
    mock_config['MY_CUSTOM_SOURCE'] = {
        'delete_after_transfer': 'false' # Explicitly False
    }
    mock_config['SETTINGS']['source_client_section'] = 'MY_CUSTOM_SOURCE'

    torrent = MockTorrent(name="Test", hash_="hash123", content_path="/src/test", save_path="/src")
    torrent.hash = torrent.hash_

    mock_src_client = MagicMock()
    mock_dest_client = MagicMock()
    mock_dest_client.get_torrent_info.return_value = torrent
    mock_dest_client.wait_for_recheck.return_value = "SUCCESS"

    # --- Execute ---
    success, msg = _post_transfer_actions(
        torrent=torrent,
        source_client=mock_src_client,
        destination_client=mock_dest_client,
        config=mock_config,
        tracker_rules={},
        ssh_connection_pools={},
        dest_content_path="/remote/downloads/test",
        destination_save_path="/remote/downloads-docker",
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

    # --- Assert ---
    # Verify delete_torrent was NOT called
    mock_src_client.delete_torrent.assert_not_called()
