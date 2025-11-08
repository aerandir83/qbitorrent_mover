import configparser
from pathlib import Path
import shutil
import logging
import sys
import configupdater
import time
from typing import List


def update_config(config_path: str, template_path: str) -> None:
    """Updates the config.ini from the template, preserving existing values.

    This function compares the user's configuration file with a template.
    It adds any new sections or options from the template to the user's
    config file while preserving existing values and comments. If changes
    are made, a timestamped backup of the original config file is created.

    Args:
        config_path: The path to the user's configuration file (e.g., 'config.ini').
        template_path: The path to the template configuration file (e.g., 'config.ini.template').
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
        A ConfigParser object loaded with the configuration.

    Raises:
        SystemExit: If the configuration file is not found.
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
    """Validates the structure and values of the configuration file.

    This class checks for required sections and options, validates transfer modes,
    checks path formats, and ensures that numeric values are within a valid
        range.

    Attributes:
        config: The ConfigParser object to validate.
        errors: A list of critical error messages found during validation.
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

    VALID_TRANSFER_MODES = ['sftp', 'rsync', 'sftp_upload', 'rsync_upload']

    def __init__(self, config: configparser.ConfigParser):
        """Initializes the ConfigValidator.

        Args:
            config: A ConfigParser object loaded with the configuration to be validated.
        """
        self.config = config
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def validate(self) -> bool:
        """Runs all validation checks and prints errors or warnings.

        This method orchestrates the validation process by calling various
        private check methods. If any errors are found, they are printed to
        stderr, and the method returns False. Warnings are printed to stderr,
        but do not cause validation to fail.

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
        """Ensure required sections exist."""
        for section in self.REQUIRED_SECTIONS:
            if not self.config.has_section(section):
                self.errors.append(f"Missing required section: [{section}]")

    def _check_required_options(self) -> None:
        """Ensure required options exist in each section."""
        for section, options in self.REQUIRED_SECTIONS.items():
            if not self.config.has_section(section):
                continue
            for option in options:
                if not self.config.has_option(section, option):
                    self.errors.append(f"Missing option '{option}' in [{section}]")
                elif not self.config.get(section, option).strip():
                    self.errors.append(f"Option '{option}' in [{section}] is empty")

    def _check_transfer_mode(self) -> None:
        """Validate transfer mode and related requirements."""
        if not self.config.has_section('SETTINGS'):
            return

        mode = self.config.get('SETTINGS', 'transfer_mode', fallback='sftp').lower()
        if mode not in self.VALID_TRANSFER_MODES:
            self.errors.append(f"Invalid transfer_mode '{mode}'. Must be one of: {', '.join(self.VALID_TRANSFER_MODES)}")

        # Check mode-specific requirements
        if mode in ['sftp_upload', 'rsync_upload'] and not self.config.has_section('DESTINATION_SERVER'):
            self.errors.append(f"transfer_mode='{mode}' requires a [DESTINATION_SERVER] section")

        if mode == 'rsync' and not shutil.which('rsync'):
            self.warnings.append("rsync mode selected but 'rsync' command not found in PATH")

    def _check_paths(self) -> None:
        """Validate path configurations."""
        if not self.config.has_section('DESTINATION_PATHS'):
            return

        dest_path = self.config.get('DESTINATION_PATHS', 'destination_path', fallback='')
        if dest_path and not Path(dest_path).is_absolute():
            self.warnings.append(f"destination_path '{dest_path}' is not an absolute path")

    def _check_numeric_values(self) -> None:
        """Validate numeric configuration values."""
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
                    if value < min_val or value > max_val:
                        self.warnings.append(
                            f"{option}={value} is outside recommended range [{min_val}-{max_val}]"
                        )
                except ValueError:
                    self.errors.append(f"Option '{option}' must be an integer")

    def _check_client_sections_exist(self) -> None:
        """Verify that referenced client sections exist."""
        if not self.config.has_section('SETTINGS'):
            return

        source_section = self.config.get('SETTINGS', 'source_client_section', fallback='')
        dest_section = self.config.get('SETTINGS', 'destination_client_section', fallback='')

        if source_section and not self.config.has_section(source_section):
            self.errors.append(f"source_client_section references non-existent section [{source_section}]")

        if dest_section and not self.config.has_section(dest_section):
            self.errors.append(f"destination_client_section references non-existent section [{dest_section}]")
