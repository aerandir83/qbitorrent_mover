import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from torrent_mover import torrent_mover
from torrent_mover import utils

class TestTrackerRules(unittest.TestCase):
    """Test tracker-based categorization logic."""

    def test_get_tracker_domain(self):
        """Test extraction of domain from tracker URL."""
        cases = [
            ("https://tracker.example.com:443/announce", "tracker.example.com"),
            ("http://announce.tracker.org/announce", "announce.tracker.org"),
            ("udp://tracker.opentrackr.org:1337/announce", "tracker.opentrackr.org"),
        ]

        for url, expected in cases:
            result = torrent_mover.get_tracker_domain(url)
            self.assertEqual(result, expected, f"Failed for {url}")

    def test_get_category_from_rules(self):
        """Test category matching from rules."""
        mock_client = Mock()
        mock_client.torrents_trackers.return_value = [
            {'url': 'https://tracker.example.com/announce'},
            {'url': 'https://backup.example.org/announce'}
        ]

        mock_torrent = Mock(hash="abc123", name="Test Torrent")

        rules = {
            "tracker.example.com": "movies",
            "backup.example.org": "tv-shows"
        }

        result = torrent_mover.get_category_from_rules(mock_torrent, rules, mock_client)
        self.assertEqual(result, "movies") # Should match first tracker


class TestPathHandling(unittest.TestCase):
    """Test path manipulation functions."""

    def test_sftp_mkdir_p_creates_nested_dirs(self):
        """Test that sftp_mkdir_p creates nested directories."""
        mock_sftp = Mock()
        mock_sftp.stat.side_effect = FileNotFoundError()

        torrent_mover.sftp_mkdir_p(mock_sftp, "/path/to/nested/dir")

        # Should create all parent directories
        calls = [call[0][0] for call in mock_sftp.mkdir.call_args_list]
        self.assertIn("/path", calls)
        self.assertIn("/path/to", calls)
        self.assertIn("/path/to/nested", calls)
        self.assertIn("/path/to/nested/dir", calls)

    def test_sftp_mkdir_p_handles_existing_dirs(self):
        """Test that existing directories don't cause errors."""
        mock_sftp = Mock()
        mock_sftp.stat.return_value = Mock() # Directory exists

        # Should not raise an error
        try:
            torrent_mover.sftp_mkdir_p(mock_sftp, "/existing/path")
            mock_sftp.mkdir.assert_not_called()
        except Exception as e:
            self.fail(f"Unexpected exception: {e}")


class TestConfigValidation(unittest.TestCase):
    """Test configuration validation."""

    def test_missing_required_section(self):
        """Test that missing required sections are detected."""
        import configparser
        config = configparser.ConfigParser()
        config['SETTINGS'] = {'transfer_mode': 'sftp'}
        # Missing SOURCE_CLIENT section
        validator = utils.ConfigValidator(config)
        self.assertFalse(validator.validate())
        self.assertIn("Missing required section: [SOURCE_CLIENT]", validator.errors)

    def test_invalid_transfer_mode(self):
        """Test that invalid transfer modes are rejected."""
        import configparser
        config = configparser.ConfigParser()
        config['SETTINGS'] = {'transfer_mode': 'invalid_mode'}

        # Should detect invalid mode
        validator = utils.ConfigValidator(config)
        self.assertFalse(validator.validate())
        self.assertIn("Invalid transfer_mode 'invalid_mode'. Must be one of: sftp, rsync, sftp_upload", validator.errors)


class TestRetryDecorator(unittest.TestCase):
    """Test the retry decorator."""

    @patch('time.sleep') # Don't actually sleep during tests
    def test_retry_succeeds_after_failures(self, mock_sleep):
        """Test that retry works after initial failures."""
        from torrent_mover.utils import retry

        mock_func = Mock(side_effect=[Exception("Fail 1"), Exception("Fail 2"), "Success"])
        mock_func.__name__ = 'mock_func'
        decorated = retry(tries=3, delay=1)(mock_func)

        result = decorated()

        self.assertEqual(result, "Success")
        self.assertEqual(mock_func.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch('time.sleep')
    def test_retry_raises_after_max_attempts(self, mock_sleep):
        """Test that retry raises exception after max attempts."""
        from torrent_mover.utils import retry

        mock_func = Mock(side_effect=Exception("Always fails"))
        mock_func.__name__ = 'mock_func'
        decorated = retry(tries=2, delay=1)(mock_func)

        with self.assertRaises(Exception):
            decorated()

        self.assertEqual(mock_func.call_count, 2)


if __name__ == '__main__':
    unittest.main()
