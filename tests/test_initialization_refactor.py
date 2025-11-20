import pytest
from unittest.mock import MagicMock, patch, call
from pathlib import Path
import configparser
import argparse
from torrent_mover import TorrentMover

@pytest.fixture
def mock_config():
    config = configparser.ConfigParser()
    config['SETTINGS'] = {
        'source_client_section': 'SOURCE',
        'destination_client_section': 'DESTINATION',
        'category_to_move': 'test_cat',
        'size_threshold_gb': '0',
        'source_server_section': 'SOURCE_SERVER',
        'transfer_mode': 'sftp'
    }
    config['SOURCE'] = {'host': 'src'}
    config['DESTINATION'] = {'host': 'dst'}
    config['SOURCE_SERVER'] = {'host': 'ssh'}
    config['DESTINATION_SERVER'] = {'host': 'ssh'}
    return config

@pytest.fixture
def mock_mover(mock_config):
    args = argparse.Namespace(dry_run=False, parallel_jobs=1)
    config_manager = MagicMock()
    config_manager.config = mock_config
    mover = TorrentMover(args, config_manager, Path('.'))

    # Mock internal components
    mover.checkpoint = MagicMock()
    mover.checkpoint.is_recheck_failed.return_value = False
    mover.file_tracker = MagicMock()

    # Fix SSH Pool mocks to support context manager unpacking
    source_pool = MagicMock()
    source_pool.get_connection.return_value.__enter__.return_value = (MagicMock(), MagicMock())
    dest_pool = MagicMock()
    dest_pool.get_connection.return_value.__enter__.return_value = (MagicMock(), MagicMock())

    mover.ssh_connection_pools = {'SOURCE_SERVER': source_pool, 'DESTINATION_SERVER': dest_pool}

    return mover

@patch('torrent_mover.print') # Mock built-in print
@patch('torrent_mover.connect_qbit')
@patch('torrent_mover.cleanup_orphaned_cache')
@patch('torrent_mover.recover_cached_torrents')
@patch('torrent_mover.get_eligible_torrents')
@patch('torrent_mover.batch_get_remote_sizes')
@patch('torrent_mover.destination_health_check')
def test_pre_flight_checks_success(
    mock_health_check, mock_batch_sizes, mock_get_eligible, mock_recover, mock_cleanup, mock_connect, mock_print,
    mock_mover
):
    # Setup mocks
    mock_connect.side_effect = [MagicMock(name="Source"), MagicMock(name="Dest")]
    mock_recover.return_value = []

    torrent = MagicMock()
    torrent.hash = "hash1"
    torrent.name = "Test Torrent"
    torrent.content_path = "/path/to/torrent"
    mock_get_eligible.return_value = [torrent]

    mock_batch_sizes.return_value = {"/path/to/torrent": 1024}
    mock_health_check.return_value = True

    # Run
    result = mock_mover.pre_flight_checks()

    # Assertions
    assert result is not None
    analyzed, total_size, count = result
    assert len(analyzed) == 1
    assert total_size == 1024
    assert count == 1

    # Check prints - using mock_print calls
    # Since we used print(..., end=" ", flush=True), we check calls.
    # Note: print mocks might be tricky with 'end' arg.

    expected_calls = [
        call("Checking configuration...", end=" ", flush=True),
        call("[OK]"),
        call("Connecting to Source Client...", end=" ", flush=True),
        call("[OK]"),
        call("Connecting to Destination Client...", end=" ", flush=True),
        call("[OK]"),
        # cleanup/recovery logs, not printed
        # eligible fetched
        call("Analyzing eligible torrents...", end=" ", flush=True),
        call("[OK]"),
        call("Checking Destination Disk Space...", end=" ", flush=True),
        call("[OK]")
    ]

    mock_print.assert_has_calls(expected_calls, any_order=False)

@patch('torrent_mover.print')
@patch('torrent_mover.connect_qbit')
def test_pre_flight_checks_fail_connect(mock_connect, mock_print, mock_mover):
    mock_connect.side_effect = Exception("Connection failed")

    result = mock_mover.pre_flight_checks()

    assert result is None

    mock_print.assert_has_calls([
        call("Checking configuration...", end=" ", flush=True),
        call("[OK]"),
        call("Connecting to Source Client...", end=" ", flush=True),
        call("[FAILED]")
    ])

@patch('torrent_mover.print')
@patch('torrent_mover.connect_qbit')
@patch('torrent_mover.get_eligible_torrents')
@patch('torrent_mover.recover_cached_torrents')
@patch('torrent_mover.cleanup_orphaned_cache')
@patch('torrent_mover.batch_get_remote_sizes')
@patch('torrent_mover.destination_health_check')
def test_pre_flight_checks_fail_health(
    mock_health, mock_sizes, mock_cleanup, mock_recover, mock_eligible, mock_connect, mock_print, mock_mover
):
    mock_connect.return_value = MagicMock()
    mock_recover.return_value = []
    mock_eligible.return_value = [MagicMock(content_path="/path", hash="h")]
    mock_sizes.return_value = {"/path": 100}
    mock_health.return_value = False # Health check fails

    result = mock_mover.pre_flight_checks()

    assert result is None
    mock_print.assert_has_calls([
        call("Checking Destination Disk Space...", end=" ", flush=True),
        call("[FAILED]")
    ])
