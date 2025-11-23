import unittest
import configparser
from config_manager import ConfigValidator

class TestConfigValidation(unittest.TestCase):
    def setUp(self):
        self.config = configparser.ConfigParser()
        # Setup minimal valid config
        self.config.add_section('SOURCE_CLIENT')
        self.config['SOURCE_CLIENT'] = {'host': 'h', 'port': '1', 'username': 'u', 'password': 'p'}
        self.config.add_section('DESTINATION_CLIENT')
        self.config['DESTINATION_CLIENT'] = {'host': 'h', 'port': '1', 'username': 'u', 'password': 'p'}
        self.config.add_section('SOURCE_SERVER')
        self.config['SOURCE_SERVER'] = {'host': 'h', 'port': '1', 'username': 'u', 'password': 'p'}
        self.config.add_section('DESTINATION_PATHS')
        self.config['DESTINATION_PATHS'] = {'destination_path': '/tmp'}
        self.config.add_section('SETTINGS')
        self.config['SETTINGS'] = {
            'source_client_section': 'SOURCE_CLIENT',
            'destination_client_section': 'DESTINATION_CLIENT',
            'category_to_move': 'cat',
            'transfer_mode': 'sftp'
        }
        self.config.add_section('General')

    def test_recheck_stopped_timeout_valid(self):
        self.config['General']['recheck_stopped_timeout'] = '5'
        validator = ConfigValidator(self.config)
        self.assertTrue(validator.validate())
        self.assertEqual(validator.warnings, [])

    def test_recheck_stopped_timeout_invalid_low(self):
        self.config['General']['recheck_stopped_timeout'] = '0'
        validator = ConfigValidator(self.config)
        validator.validate()
        # Expect warning about range (1-300)
        warnings = [w for w in validator.warnings if 'recheck_stopped_timeout' in w]
        self.assertTrue(warnings, "Expected warning for recheck_stopped_timeout=0")

    def test_recheck_stopped_timeout_invalid_high(self):
        self.config['General']['recheck_stopped_timeout'] = '301'
        validator = ConfigValidator(self.config)
        validator.validate()
        warnings = [w for w in validator.warnings if 'recheck_stopped_timeout' in w]
        self.assertTrue(warnings, "Expected warning for recheck_stopped_timeout=301")

    def test_recheck_grace_period_valid(self):
        self.config['General']['recheck_grace_period'] = '60'
        validator = ConfigValidator(self.config)
        self.assertTrue(validator.validate())
        self.assertEqual(validator.warnings, [])

    def test_recheck_grace_period_invalid_range(self):
        self.config['General']['recheck_grace_period'] = '601'
        validator = ConfigValidator(self.config)
        validator.validate()
        warnings = [w for w in validator.warnings if 'recheck_grace_period' in w]
        self.assertTrue(warnings, "Expected warning for recheck_grace_period=601")
