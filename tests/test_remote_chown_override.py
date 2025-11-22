import pytest
from unittest.mock import MagicMock, patch
from torrent_mover import _post_transfer_actions

@patch('torrent_mover.change_ownership')
def test_rsync_mode_triggers_remote_chown_with_destination_server(mock_change_ownership):
    """
    Verifies that even when transfer_mode is 'rsync' (normally local),
    the presence of DESTINATION_SERVER triggers a remote chown attempt.
    """
    # Setup
    mock_change_ownership.return_value = True

    mock_torrent = MagicMock()
    mock_torrent.name = "Test Torrent"
    mock_torrent.hash = "hash123"

    # Config with rsync mode AND Destination Server
    mock_dest_server_config = {'host': 'remote', 'username': 'user'}

    def config_getitem(key):
        if key == 'SETTINGS':
            return {'transfer_mode': 'rsync', 'chown_user': 'u', 'chown_group': 'g'}
        if key == 'DESTINATION_SERVER':
            return mock_dest_server_config
        if key == 'DESTINATION_PATHS':
            return {'destination_path': '/dest', 'remote_destination_path': '/remote'}
        if key == 'DESTINATION_CLIENT':
            return {'add_torrents_paused': 'false', 'start_torrents_after_recheck': 'false'}
        if key == 'SOURCE_CLIENT':
            return {'delete_after_transfer': 'false'}
        return {}

    mock_config = MagicMock()
    mock_config.__getitem__.side_effect = config_getitem
    # Mock __contains__ to return True for DESTINATION_SERVER
    mock_config.__contains__.side_effect = lambda key: key in ['SETTINGS', 'DESTINATION_SERVER', 'DESTINATION_PATHS', 'DESTINATION_CLIENT', 'SOURCE_CLIENT']
    # Mock has_section specifically for DESTINATION_SERVER
    mock_config.has_section.side_effect = lambda section: section == 'DESTINATION_SERVER'
    mock_config.getboolean.return_value = False

    # Make sure accessing DESTINATION_SERVER returns the mock config object (SectionProxy)
    # In the lambda above we returned a dict, but the code might expect the object itself if it iterates it.
    # However, code uses config['DESTINATION_SERVER'] which triggers __getitem__.

    mock_dest_client = MagicMock()
    mock_dest_client.get_torrent_info.return_value = None
    mock_dest_client.wait_for_recheck.return_value = "SUCCESS"

    mock_ssh_pools = {'DESTINATION_SERVER': MagicMock()}

    # Execute
    result, msg = _post_transfer_actions(
        torrent=mock_torrent,
        source_client=MagicMock(),
        destination_client=mock_dest_client,
        config=mock_config,
        tracker_rules={},
        ssh_connection_pools=mock_ssh_pools,
        dest_content_path="/dest/content",
        destination_save_path="/remote/content",
        transfer_executed=True,
        dry_run=False,
        test_run=False,
        file_tracker=MagicMock(),
        transfer_mode="rsync",
        all_files=[],
        ui=MagicMock(),
        log_transfer=MagicMock(),
        _update_transfer_progress=MagicMock()
    )

    # Assert
    assert result is True

    # Verify change_ownership was called with remote_config matching mock_dest_server_config
    # args: (path_to_chown, chown_user, chown_group, remote_config, dry_run, ssh_connection_pools)
    args, kwargs = mock_change_ownership.call_args
    remote_config_arg = args[3]

    assert remote_config_arg == mock_dest_server_config
    assert args[0] == "/dest/content"
    assert args[5] == mock_ssh_pools
