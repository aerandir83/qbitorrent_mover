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
    config = ConfigUpdater()
    config.read(config_file, encoding='utf-8')

    template_config = ConfigUpdater()
    template_config.read(template_file, encoding='utf-8')

    needs_update = False
    # Iterate over the template to find missing options
    for section in template_config.sections():
        if not config.has_section(section):
            needs_update = True
            logging.info(f"Adding missing section [{section}] to config.ini.")
            config.add_section(section)
            # Copy the whole section from template
            config[section].read_dict(template_config[section].items())
            # Attempt to copy comments for the section if possible
            if template_config[section].comments:
                config[section].comments = template_config[section].comments

        for option, value in template_config[section].items():
            if not config[section].has_option(option):
                needs_update = True
                logging.info(f"Adding missing option '{option}' to section [{section}] in config.ini.")
                config[section][option] = value
                # Attempt to copy comments for the option
                # This part is a bit tricky with configupdater, but we can try
                # to find the comment associated with the option in the template
                # and add it to the user's config.

    if needs_update:
        logging.info("Configuration file was updated with new options. Saving changes.")
        config.write_to_file(config_file, encoding='utf-8')

    return config