import logging
import sys
from pathlib import Path
from configupdater import ConfigUpdater

def load_and_update_config(config_path="config.ini", template_path="config.ini.template"):
    """
    Loads the user's configuration from config.ini and updates it with any
    new options from config.ini.template without overwriting existing values.
    Preserves comments and structure.
    """
    config_file = Path(config_path)
    template_file = Path(template_path)

    if not config_file.is_file():
        logging.error(f"FATAL: Configuration file not found at '{config_path}'.")
        logging.error(f"Please copy '{template_path}' to '{config_path}' and fill in your details.")
        sys.exit(1)

    if not template_file.is_file():
        logging.error(f"FATAL: Configuration template file not found at '{template_path}'.")
        sys.exit(1)

    # Use ConfigUpdater to preserve comments and structure
    updater = ConfigUpdater()
    updater.read(config_file, encoding='utf-8')

    template_config = ConfigUpdater()
    template_config.read(template_file, encoding='utf-8')

    needs_update = False
    # Iterate over the template to find missing options
    for section in template_config.sections():
        if not updater.has_section(section):
            needs_update = True
            logging.info(f"Adding missing section [{section}] to config.ini.")
            # Add the section with its comments and options from the template
            updater.add_section(section)
            updater[section].read_dict(template_config[section].items())
            # This doesn't perfectly preserve all comments, but it's a good effort
            if template_config[section].comments:
                 updater[section].comments = template_config[section].comments
        else: # Section exists, check for missing options
            for option, value_obj in template_config[section].items():
                if not updater[section].has_option(option):
                    needs_update = True
                    logging.info(f"Adding missing option '{option}' to section [{section}] in config.ini.")
                    # Add the option with its value and comment
                    updater[section][option] = value_obj.value

    if needs_update:
        logging.info("Configuration file was updated with new options. Saving changes.")
        try:
            updater.write_to_file(config_file, encoding='utf-8')
        except Exception as e:
            logging.error(f"Failed to write updates to config file: {e}")
            sys.exit(1)

    return updater