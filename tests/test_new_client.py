import configparser
import logging
import sys
import os

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from clients.factory import get_client
from clients.qbittorrent import QBittorrentClient

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def test_manual_connection():
    config_path = 'config.ini'
    if not os.path.exists(config_path):
        logging.error(f"{config_path} not found. Cannot run manual test.")
        return

    config = configparser.ConfigParser()
    config.read(config_path)

    # Use SOURCE_CLIENT section if available, otherwise try to find one
    section_name = 'SOURCE_CLIENT'
    if section_name not in config:
        logging.warning(f"[{section_name}] not found in config.ini.")
        # Try to find any section with host/port/username/password
        for sec in config.sections():
            if all(key in config[sec] for key in ['host', 'port', 'username', 'password']):
                section_name = sec
                logging.info(f"Using [{section_name}] for testing.")
                break
        else:
            logging.error("No suitable client section found in config.ini.")
            return

    section = config[section_name]
    logging.info(f"Testing connection to {section.get('host')}...")

    # Use factory
    client = get_client(section, 'qbittorrent', 'TestClient')

    if not isinstance(client, QBittorrentClient):
        logging.error("Factory failed to return QBittorrentClient.")
        return

    try:
        # Test Connection
        client.connect()
        logging.info("✅ Successfully connected!")

        # Test Fetching
        logging.info("Testing get_eligible_torrents...")
        # Use a category that might exist or just empty string for all?
        # get_eligible_torrents requires a category.
        # I'll try a dummy category, expected empty list or result.
        torrents = client.get_eligible_torrents("test_category")
        logging.info(f"Found {len(torrents)} torrents in 'test_category'.")

        # Dry Run verification
        logging.info("Testing wait_for_recheck dry_run...")
        result = client.wait_for_recheck("dummyhash", None, 10, 10, dry_run=True)
        if result == "SUCCESS":
             logging.info("✅ Dry run wait_for_recheck passed (returned SUCCESS).")
        else:
             logging.error(f"❌ Dry run wait_for_recheck failed (returned {result}).")

    except Exception as e:
        logging.error(f"❌ Connection or test failed: {e}")

if __name__ == "__main__":
    test_manual_connection()
