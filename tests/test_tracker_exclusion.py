import pytest
from unittest.mock import MagicMock, patch
import configparser
import argparse
from pathlib import Path
from torrent_mover import TorrentMover

# --- Mocks ---
from tests.mocks.mock_qbittorrent import MockTorrent

@pytest.fixture
def mock_config():
    """Provides a basic mock ConfigParser object."""
    config = configparser.ConfigParser()
    config['SETTINGS'] = {
        'transfer_mode': 'rsync',
        'source_client_section': 'SOURCE_CLIENT',
        'destination_client_section': 'DESTINATION_CLIENT',
        'source_server_section': 'SOURCE_SERVER',
        'category_to_move': 'complete'
    }
    config['DESTINATION_PATHS'] = {'destination_path': '/dst'}
    config['SOURCE_CLIENT'] = {}
    config['DESTINATION_CLIENT'] = {}
    config['SOURCE_SERVER'] = {'host': 'host', 'port': '22', 'username': 'user', 'password': 'pass', 'max_concurrent_ssh_sessions': '1'}
    return config

@pytest.fixture
def mock_args():
    return argparse.Namespace(
        dry_run=False,
        test_run=False,
        parallel_jobs=1,
        debug=False
    )

@patch('torrent_mover.destination_health_check')
@patch('torrent_mover.get_category_from_rules')
@patch('torrent_mover.batch_get_remote_sizes')
@patch('torrent_mover.get_client')
@patch('torrent_mover.SSHConnectionPool')
def test_exclusion_logic(mock_ssh_pool_cls, mock_get_client, mock_batch_sizes, mock_get_cat, mock_health_check, mock_config, mock_args):
    mock_health_check.return_value = True
    # Setup
    mock_src_client = MagicMock()
    mock_src_client.connect.return_value = True
    mock_src_client.client = MagicMock() # Underlying client for rules check

    mock_dst_client = MagicMock()
    mock_dst_client.connect.return_value = True

    mock_get_client.side_effect = [mock_src_client, mock_dst_client]

    t1 = MockTorrent(name="T1", hash_="h1", content_path="/p1", save_path="/s1")
    t2 = MockTorrent(name="T2", hash_="h2", content_path="/p2", save_path="/s2")
    t1.hash = "h1"
    t2.hash = "h2"

    # Mocking get_eligible_torrents to return our test torrents
    mock_src_client.get_eligible_torrents.return_value = [t1, t2]

    # Mocking get_category_from_rules to ignore T2
    def side_effect_get_cat(torrent, rules, client):
        if torrent.name == "T2":
            return "ignore"
        return "some_other_category"

    mock_get_cat.side_effect = side_effect_get_cat

    # Mocking batch_get_remote_sizes
    # It should receive only /p1 if exclusion works.
    # If not, it might receive /p1 and /p2.
    # We return sizes for both so it doesn't crash, but verify calls later.
    mock_batch_sizes.return_value = {"/p1": 100, "/p2": 100}

    # Mock SSH Pool instance
    mock_pool = MagicMock()
    mock_ssh_pool_cls.return_value = mock_pool
    # Mock context manager for connection
    mock_pool.get_connection.return_value.__enter__.return_value = (MagicMock(), MagicMock())

    mock_config_manager = MagicMock()
    mock_config_manager.config = mock_config

    mover = TorrentMover(args=mock_args, config_manager=mock_config_manager, script_dir=Path("."))

    # Mock internals
    mover.checkpoint = MagicMock()
    mover.file_tracker = MagicMock()
    mover.tracker_rules = {"tracker.com": "ignore"} # dummy rules
    mover.checkpoint.is_recheck_failed.return_value = False
    mover.ssh_connection_pools = {'SOURCE_SERVER': mock_pool} # Manually set pool

    # Run pre_flight_checks
    result = mover.pre_flight_checks()

    assert result is not None
    analyzed_torrents, total_size, total_count = result

    # Verify T1 is in, T2 is out of analyzed_torrents
    assert len(analyzed_torrents) == 1
    assert analyzed_torrents[0][0].name == "T1"

    # Verify batch_get_remote_sizes was called only for T1's path
    # Note: T2 is filtered BEFORE analysis, so its path should not be in the call
    assert mock_batch_sizes.called
    calls = mock_batch_sizes.call_args_list
    # The argument to batch_get_remote_sizes is (ssh, paths)
    paths_checked = calls[0][0][1]
    assert "/p1" in paths_checked
    assert "/p2" not in paths_checked
