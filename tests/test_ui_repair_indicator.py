import unittest
from unittest.mock import MagicMock, patch
from ui import UIManagerV2, SimpleUIManager, BaseUIManager
import inspect
import logging

class TestUIRepairIndicator(unittest.TestCase):
    def test_base_ui_manager_signature(self):
        # This checks if we updated the abstract method correctly
        sig = inspect.signature(BaseUIManager.start_torrent_transfer)
        self.assertIn('is_repair', sig.parameters, "is_repair argument missing in BaseUIManager")
        self.assertEqual(sig.parameters['is_repair'].default, False, "Default value for is_repair should be False")

    def test_simple_ui_manager_logs_repair(self):
        ui = SimpleUIManager()
        # We need to configure logging to capture it, but assertLogs does that

        with self.assertLogs(level='INFO') as cm:
            ui.start_torrent_transfer(
                torrent_hash="hash1",
                torrent_name="Test Torrent",
                total_size=1000,
                all_files=["file1"],
                transfer_multiplier=1,
                is_repair=True
            )

        # We expect a distinctive message
        found = any("REPAIR" in output for output in cm.output)
        self.assertTrue(found, f"Did not find 'REPAIR' in logs: {cm.output}")

    def test_ui_manager_v2_stores_state(self):
        # We mock the console to prevent actual output during init
        mock_console = MagicMock()
        mock_console.width = 100 # Set a default width

        with patch('ui.Console', return_value=mock_console):
            ui = UIManagerV2()
            ui.start_torrent_transfer(
                torrent_hash="hash1",
                torrent_name="Test Torrent",
                total_size=1000,
                all_files=["file1"],
                transfer_multiplier=1,
                is_repair=True
            )

            self.assertIn("hash1", ui._torrents)
            self.assertTrue(ui._torrents["hash1"].get("is_repair", False))

            ui.start_torrent_transfer(
                torrent_hash="hash2",
                torrent_name="Test Torrent 2",
                total_size=1000,
                all_files=["file1"],
                transfer_multiplier=1
                # Default is_repair=False
            )

            self.assertIn("hash2", ui._torrents)
            self.assertFalse(ui._torrents["hash2"].get("is_repair", False))

if __name__ == '__main__':
    unittest.main()
