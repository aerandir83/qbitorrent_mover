"""Manages loading, updating, and validating the application's configuration.

This module handles all interactions with the `config.ini` file. It ensures
the configuration is up-to-date with the latest template, loads it into a
`ConfigParser` object, and provides a validation class to check for common
errors.
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
    """Updates the config.ini from the template.

    This function compares the user's `config.ini` with the application's
    `config.ini.template`. It adds new sections and options from the template
    to the user's config file while preserving existing values and comments.
    It also creates a timestamped backup of the original config file before
    making any changes.

    Args:
        config_path: The path to the user's `config.ini` file.
        template_path: The path to the `config.ini.template` file.
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

    If the file is not found, it logs a fatal error and exits the application.

    Args:
        config_path: The path to the configuration file. Defaults to "config.ini".

    Returns:
        A `configparser.ConfigParser` object loaded with the configuration.
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
    """Validates configuration file structure and values.

    This class checks for required sections, options, and valid values to
    ensure the application can run correctly. It accumulates all errors and
    warnings before reporting them.

    Attributes:
        config: The `ConfigParser` object to validate.
        errors: A list of critical error messages.
        warnings: A list of non-critical warning messages.
    """

    REQUIRED_SECTIONS = {
        'SOURCE_CLIENT': ['host', 'port', 'username', 'password'],
        'DESTINATION_CLIENT': ['host', 'port', 'username', 'password'],
        'SOURCE_SERVER': ['host', 'port', 'username', 'password'],
        'DESTINATION_PATHS': ['destination_path'],
        'SETTINGS': ['source_client_section', 'destination_client_section',
                     'category_to_move', 'transfer_mode']
    }

    VALID_TRANSFER_MODES = ['sftp', 'rsync', 'sftp_upload']

    def __init__(self, config: configparser.ConfigParser):
        """Initializes the ConfigValidator.

        Args:
            config: The `configparser.ConfigParser` instance to be validated.
        """
        self.config = config
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def validate(self) -> bool:
        """Runs all validation checks and prints the results.

        This method executes all individual validation checks. If any errors are
        found, it prints them to stderr and returns False. If only warnings
        are found, they are printed and the method returns True.

        Returns:
            True if the configuration is valid (no errors), False otherwise.
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
        """Ensures all required sections exist in the configuration."""
        for section in self.REQUIRED_SECTIONS:
            if not self.config.has_section(section):
                self.errors.append(f"Missing required section: [{section}]")

    def _check_required_options(self) -> None:
        """Ensures all required options exist and are not empty."""
        for section, options in self.REQUIRED_SECTIONS.items():
            if not self.config.has_section(section):
                continue
            for option in options:
                if not self.config.has_option(section, option):
                    self.errors.append(f"Missing option '{option}' in [{section}]")
                elif not self.config.get(section, option).strip():
                    self.errors.append(f"Option '{option}' in [{section}] is empty")

    def _check_transfer_mode(self) -> None:
        """Validates 'transfer_mode' and its dependent sections."""
        if not self.config.has_section('SETTINGS'):
            return

        mode = self.config.get('SETTINGS', 'transfer_mode', fallback='sftp').lower()
        if mode not in self.VALID_TRANSFER_MODES:
            self.errors.append(f"Invalid transfer_mode '{mode}'. Must be one of: {', '.join(self.VALID_TRANSFER_MODES)}")

        # Check mode-specific requirements
        if mode == 'sftp_upload' and not self.config.has_section('DESTINATION_SERVER'):
            self.errors.append("transfer_mode='sftp_upload' requires [DESTINATION_SERVER] section")

        if mode == 'rsync' and not shutil.which('rsync'):
            self.warnings.append("rsync mode selected but 'rsync' command not found in PATH")

    def _check_paths(self) -> None:
        """Validates path-related configurations."""
        if not self.config.has_section('DESTINATION_PATHS'):
            return

        dest_path = self.config.get('DESTINATION_PATHS', 'destination_path', fallback='')
        if dest_path and not Path(dest_path).is_absolute():
            self.warnings.append(f"destination_path '{dest_path}' is not an absolute path")

    def _check_numeric_values(self) -> None:
        """Validates that certain configuration values are numeric."""
        if not self.config.has_section('SETTINGS'):
            return

        numeric_options = {
            'max_concurrent_downloads': (1, 20),
            'max_concurrent_uploads': (1, 20),
        }

        for option, (min_val, max_val) in numeric_options.items():
            if self.config.has_option('SETTINGS', option):
                try:
                    value = self.config.getint('SETTINGS', option)
                    if value < min_val or value > max_val:
                        self.warnings.append(
                            f"{option}={value} is outside recommended range [{min_val}-{max_val}]"
                        )
                except ValueError:
                    self.errors.append(f"Option '{option}' must be an integer")

    def _check_client_sections_exist(self) -> None:
        """Verifies that client sections referenced in [SETTINGS] exist."""
        if not self.config.has_section('SETTINGS'):
            return

        source_section = self.config.get('SETTINGS', 'source_client_section', fallback='')
        dest_section = self.config.get('SETTINGS', 'destination_client_section', fallback='')

        if source_section and not self.config.has_section(source_section):
            self.errors.append(f"source_client_section references non-existent section [{source_section}]")

        if dest_section and not self.config.has_section(dest_section):
            self.errors.append(f"destination_client_section references non-existent section [{dest_section}]")
