import unittest
from unittest.mock import MagicMock, patch
import configparser
from config_manager import ConfigManager
import logging

class TestConfigManagerWatchdog(unittest.TestCase):
    def setUp(self):
        # Suppress logging during tests
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    def test_get_watchdog_timeout_default(self):
        # Test missing section/option returns 300
        config = configparser.ConfigParser()
        # No General section

        with patch('config_manager.ConfigManager._load_config', return_value=config), \
             patch('config_manager.ConfigManager._validate_config'):
            cm = ConfigManager("dummy.ini")
            self.assertEqual(cm.get_watchdog_timeout(), 300)

    def test_get_watchdog_timeout_valid(self):
        config = configparser.ConfigParser()
        config.add_section('General')
        config['General']['watchdog_timeout'] = '600'

        with patch('config_manager.ConfigManager._load_config', return_value=config), \
             patch('config_manager.ConfigManager._validate_config'):
            cm = ConfigManager("dummy.ini")
            self.assertEqual(cm.get_watchdog_timeout(), 600)

    def test_get_watchdog_timeout_invalid_value(self):
        config = configparser.ConfigParser()
        config.add_section('General')
        config['General']['watchdog_timeout'] = 'invalid'

        with patch('config_manager.ConfigManager._load_config', return_value=config), \
             patch('config_manager.ConfigManager._validate_config'):
            cm = ConfigManager("dummy.ini")
            self.assertEqual(cm.get_watchdog_timeout(), 300)

    def test_get_watchdog_timeout_high_value(self):
        config = configparser.ConfigParser()
        config.add_section('General')
        config['General']['watchdog_timeout'] = '4000'

        with patch('config_manager.ConfigManager._load_config', return_value=config), \
             patch('config_manager.ConfigManager._validate_config'):
            cm = ConfigManager("dummy.ini")
            # Should return the value but log a warning
            self.assertEqual(cm.get_watchdog_timeout(), 4000)
