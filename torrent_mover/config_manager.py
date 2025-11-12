"""Manages loading, updating, and validating the application's configuration.

This module is responsible for handling the `config.ini` file. It includes
functionality to:
- Create a new configuration file from a template if one doesn't exist.
- Update an existing configuration file with new options from the template
  while preserving user-defined values.
- Load the configuration into a `ConfigParser` object for use by the application.
- Validate the configuration to ensure all required sections and options are
  present and have valid values.
"""
import configparser
from pathlib import Path
import shutil
import logging
import sys
import configupdater
import time
from typing import List


def update_config(config_path: str, template_path: str) -> None:
    """Updates an existing config.ini from a template, preserving user values.

    This function compares the user's configuration file with a template. It adds
    any new sections or options present in the template to the user's config
    file. Existing user-defined values, comments, and file structure are
    preserved.

    If the configuration file is modified, a timestamped backup of the original
    file is created in a `backup` subdirectory. If no configuration file exists
    at `config_path`, one is created from the template.

    Args:
        config_path: The path to the user's configuration file (e.g., 'config.ini').
        template_path: The path to the template file (e.g., 'config.ini.template').

    Raises:
        SystemExit: If the template file cannot be found or a new config cannot be created.
    """
    config_file = Path(config_path)
    template_file = Path(template_path)
    logging.info("STATE: Checking for configuration updates...")

    if not template_file.is_file():
        logging.error(f"FATAL: Config template '{template_path}' not found.")
        sys.exit(1)

    if not config_file.is_file():
        logging.warning(f"Configuration file not found at '{config_path}'.")
        logging.warning("Creating a new one from the template. Please review and fill it out.")
        try:
            shutil.copy2(template_file, config_file)
        except OSError as e:
            logging.error(f"FATAL: Could not create config file: {e}")
            sys.exit(1)
        return

    try:
        updater = configupdater.ConfigUpdater()
        updater.read(config_file, encoding='utf-8')
        template_updater = configupdater.ConfigUpdater()
        template_updater.read(template_file, encoding='utf-8')

        changes_made = False
        for section_name in template_updater.sections():
            template_section = template_updater[section_name]
            if not updater.has_section(section_name):
                user_section = updater.add_section(section_name)
                for key, opt in template_section.items():
                    user_opt = user_section.set(key, opt.value)
                    if hasattr(opt, 'comments') and opt.comments.above:
                        user_opt.add_comment('\n'.join(opt.comments.above), above=True)
                    if hasattr(opt, 'comments') and opt.comments.inline:
                        user_opt.add_comment(opt.comments.inline, inline=True)
                changes_made = True
                logging.info(f"CONFIG: Added new section to config: [{section_name}]")
            else:
                user_section = updater[section_name]
                for key, opt in template_section.items():
                    if not user_section.has_option(key):
                        user_opt = user_section.set(key, opt.value)
                        if hasattr(opt, 'comments') and opt.comments.above:
                            user_opt.add_comment('\n'.join(opt.comments.above), above=True)
                        if hasattr(opt, 'comments') and opt.comments.inline:
                            user_opt.add_comment(opt.comments.inline, inline=True)
                        changes_made = True
                        logging.info(f"CONFIG: Added new option in [{section_name}]: {key}")

        if changes_made:
            backup_dir = config_file.parent / 'backup'
            backup_dir.mkdir(exist_ok=True)
            backup_filename = f"{config_file.stem}.bak_{time.strftime('%Y%m%d-%H%M%S')}"
            backup_path = backup_dir / backup_filename
            shutil.copy2(config_file, backup_path)
            logging.info(f"CONFIG: Backed up existing configuration to '{backup_path}'")
            with config_file.open('w', encoding='utf-8') as f:
                updater.write(f)
            logging.info("CONFIG: Configuration file has been updated with new options.")
        else:
            logging.info("CONFIG: Configuration file is already up-to-date.")
    except Exception as e:
        logging.error(f"FATAL: An error occurred during config update: {e}", exc_info=True)
        sys.exit(1)

def load_config(config_path: str = "config.ini") -> configparser.ConfigParser:
    """Loads the configuration from the specified .ini file.

    Args:
        config_path: The path to the configuration file.

    Returns:
        A `ConfigParser` object loaded with the configuration settings.

    Raises:
        SystemExit: If the configuration file does not exist at `config_path`.
    """
    config_file = Path(config_path)
    if not config_file.is_file():
        logging.error(f"FATAL: Configuration file not found at '{config_path}'.")
        logging.error("Please copy 'config.ini.template' to 'config.ini' and fill in your details.")
        sys.exit(1)
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


class ConfigValidator:
    """Validates the structure and values of the application's configuration.

    This class performs a series of checks on a `ConfigParser` object to ensure
    it meets the application's requirements. It verifies the presence of
    required sections and options, checks for valid transfer modes, validates
    path formats, and ensures that numeric values are within a sensible range.

    Attributes:
        config (configparser.ConfigParser): The configuration object to validate.
        errors (List[str]): A list of critical error messages found. If this list
            is not empty after validation, the configuration is considered invalid.
        warnings (List[str]): A list of non-critical warning messages. These
            highlight potential issues but do not invalidate the configuration.
    """

    REQUIRED_SECTIONS = {
        'SOURCE_CLIENT': ['host', 'port', 'username', 'password'],
        'DESTINATION_CLIENT': ['host', 'port', 'username', 'password'],
        'SOURCE_SERVER': ['host', 'port', 'username', 'password'],
        'DESTINATION_PATHS': ['destination_path'],
        'SETTINGS': ['source_client_section', 'destination_client_section',
                     'category_to_move', 'transfer_mode']
    }

    VALID_TRANSFER_MODES = ['sftp', 'rsync', 'sftp_upload', 'rsync_upload']

    def __init__(self, config: configparser.ConfigParser):
        """Initializes the ConfigValidator with a configuration object.

        Args:
            config: A `ConfigParser` object loaded with the configuration
                to be validated.
        """
        self.config = config
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def validate(self) -> bool:
        """Runs all validation checks and prints resulting errors or warnings.

        This method orchestrates the validation process by calling all private
        check methods. If any errors are found, they are printed to stderr, and
        the method returns `False`. Warnings are also printed but do not cause
        validation to fail.

        Returns:
            `True` if the configuration is valid (no errors), `False` otherwise.
        """
        self._check_required_sections()
        self._check_required_options()
        self._check_transfer_mode()
        self._check_paths()
        self._check_numeric_values()
        self._check_client_sections_exist()

        if self.errors:
            print("Configuration errors found:", file=sys.stderr)
            for error in self.errors:
                print(f" ❌ {error}", file=sys.stderr)
            return False

        if self.warnings:
            print("Configuration warnings:", file=sys.stderr)
            for warning in self.warnings:
                print(f" ⚠️ {warning}", file=sys.stderr)

        return True

    def _check_required_sections(self) -> None:
        """Checks if all sections defined in `REQUIRED_SECTIONS` exist in the config."""
        for section in self.REQUIRED_SECTIONS:
            if not self.config.has_section(section):
                self.errors.append(f"Missing required section: [{section}]")

    def _check_required_options(self) -> None:
        """Checks if all required options exist and are non-empty in their respective sections."""
        for section, options in self.REQUIRED_SECTIONS.items():
            if not self.config.has_section(section):
                continue
            for option in options:
                if not self.config.has_option(section, option):
                    self.errors.append(f"Missing option '{option}' in [{section}]")
                elif not self.config.get(section, option).strip():
                    self.errors.append(f"Option '{option}' in [{section}] is empty")

    def _check_transfer_mode(self) -> None:
        """Validates the 'transfer_mode' and its related section requirements."""
        if not self.config.has_section('SETTINGS'):
            return

        mode = self.config.get('SETTINGS', 'transfer_mode', fallback='sftp').lower()
        if mode not in self.VALID_TRANSFER_MODES:
            self.errors.append(f"Invalid transfer_mode '{mode}'. Must be one of: {', '.join(self.VALID_TRANSFER_MODES)}")

        # Check for mode-specific section requirements
        if mode in ['sftp_upload', 'rsync_upload'] and not self.config.has_section('DESTINATION_SERVER'):
            self.errors.append(f"transfer_mode='{mode}' requires a [DESTINATION_SERVER] section")

        # Check for external command dependencies
        if mode == 'rsync' and not shutil.which('rsync'):
            self.warnings.append("rsync mode selected but 'rsync' command not found in PATH")

    def _check_paths(self) -> None:
        """Validates that configured paths are absolute."""
        if not self.config.has_section('DESTINATION_PATHS'):
            return

        dest_path = self.config.get('DESTINATION_PATHS', 'destination_path', fallback='')
        if dest_path and not Path(dest_path).is_absolute():
            self.warnings.append(f"destination_path '{dest_path}' is not an absolute path")

    def _check_numeric_values(self) -> None:
        """Validates that numeric options are integers and within a recommended range."""
        if not self.config.has_section('SETTINGS'):
            return

        numeric_options = {
            'max_concurrent_downloads': (1, 20),
            'max_concurrent_uploads': (1, 20),
            'pool_wait_timeout': (30, 3600),
        }

        for option, (min_val, max_val) in numeric_options.items():
            if self.config.has_option('SETTINGS', option):
                try:
                    value = self.config.getint('SETTINGS', option)
                    if not (min_val <= value <= max_val):
                        self.warnings.append(
                            f"{option}={value} is outside recommended range [{min_val}-{max_val}]"
                        )
                except ValueError:
                    self.errors.append(f"Option '{option}' must be an integer")

    def _check_client_sections_exist(self) -> None:
        """Verifies that client/server sections referenced in [SETTINGS] actually exist."""
        if not self.config.has_section('SETTINGS'):
            return

        source_section = self.config.get('SETTINGS', 'source_client_section', fallback='')
        dest_section = self.config.get('SETTINGS', 'destination_client_section', fallback='')

        if source_section and not self.config.has_section(source_section):
            self.errors.append(f"source_client_section references non-existent section [{source_section}]")

        if dest_section and not self.config.has_section(dest_section):
            self.errors.append(f"destination_client_section references non-existent section [{dest_section}]")
