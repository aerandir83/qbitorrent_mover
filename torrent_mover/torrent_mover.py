# Torrent Mover
#
# A script to automatically move completed torrents from a source qBittorrent client
# to a destination client and transfer the files via SFTP.

import configparser
import sys
import logging
from pathlib import Path
import qbittorrentapi
import paramiko
import json
from urllib.parse import urlparse

# --- Configuration Loading ---

def load_config(config_path="config.ini"):
    """
    Loads the configuration from the specified .ini file.
    Exits if the file is not found.
    """
    config_file = Path(config_path)
    if not config_file.is_file():
        logging.error(f"FATAL: Configuration file not found at '{config_path}'.")
        logging.error("Please copy 'config.ini.template' to 'config.ini' and fill in your details.")
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read(config_file)
    return config

# --- Connection Functions ---

def connect_qbit(config_section, client_name):
    """
    Connects to a qBittorrent client using details from a config section.
    Returns a connected client object or None on failure.
    """
    try:
        host = config_section['host']
        port = config_section.getint('port')
        username = config_section['username']
        password = config_section['password']
        verify_cert = config_section.getboolean('verify_cert', fallback=True)

        logging.info(f"Connecting to {client_name} qBittorrent at {host}...")
        client = qbittorrentapi.Client(
            host=host,
            port=port,
            username=username,
            password=password,
            VERIFY_WEBUI_CERTIFICATE=verify_cert
        )
        client.auth_log_in()
        logging.info(f"Successfully connected to {client_name}. Version: {client.app.version}")
        return client
    except Exception as e:
        logging.error(f"Failed to connect to {client_name} qBittorrent: {e}")
        return None

def connect_sftp(config_section):
    """
    Connects to a server via SFTP using details from a config section.
    Returns a connected SFTP client and transport object, or (None, None) on failure.
    """
    try:
        host = config_section['host']
        port = config_section.getint('port')
        username = config_section['username']
        password = config_section['password']

        logging.info(f"Establishing SFTP connection to {host}...")
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        logging.info(f"Successfully established SFTP connection to {host}.")
        return sftp, transport
    except Exception as e:
        logging.error(f"Failed to establish SFTP connection: {e}")
        return None, None

import os
import sys
import time
import argparse

# --- SFTP Transfer Logic ---

def _sftp_download_file(sftp, remote_file, local_file, dry_run=False):
    """
    Downloads a single file, showing progress and performing a size check for overwriting.
    """
    local_path = Path(local_file)
    remote_stat = sftp.stat(remote_file)
    total_size = remote_stat.st_size

    if local_path.exists():
        local_size = local_path.stat().st_size
        if local_size == total_size:
            logging.info(f"Skipping file (exists and size matches): {local_path}")
            return
        else:
            logging.warning(f"Overwriting file (size mismatch remote:{total_size} vs local:{local_size}): {local_path}")

    if dry_run:
        logging.info(f"[DRY RUN] Would download: {remote_file} -> {local_path}")
        return

    local_path.parent.mkdir(parents=True, exist_ok=True)

    class ProgressTracker:
        def __init__(self, filename, total_size):
            self.filename = os.path.basename(filename)
            self.total_size = total_size
            self.start_time = time.time()
            self._last_update_time = 0

        def __call__(self, transferred, total):
            now = time.time()
            # Throttle updates to every half-second to avoid excessive printing
            if now - self._last_update_time < 0.5 and transferred != total:
                return
            self._last_update_time = now

            elapsed_time = now - self.start_time
            speed = transferred / elapsed_time if elapsed_time > 0 else 0
            speed_kbs = speed / 1024

            percent = (transferred / self.total_size) * 100 if self.total_size > 0 else 100
            transferred_mb = transferred / (1024 * 1024)
            total_mb = self.total_size / (1024 * 1024)

            # Use sys.stdout.write and \r to create a dynamic progress line
            progress_str = (
                f"\rDownloading {self.filename}: {transferred_mb:.2f}/{total_mb:.2f}MB "
                f"({percent:.1f}%) at {speed_kbs:.2f} KB/s..."
            )
            sys.stdout.write(progress_str)
            sys.stdout.flush()

    logging.info(f"Starting download: {remote_file}")
    progress_tracker = ProgressTracker(remote_file, total_size)
    try:
        sftp.get(remote_file, str(local_path), callback=progress_tracker)
        sys.stdout.write('\n')  # Newline after download completes
        sys.stdout.flush()
        logging.info(f"Download of {progress_tracker.filename} completed.")
    except Exception as e:
        sys.stdout.write('\n') # Ensure we newline on error too
        logging.error(f"Download failed for {remote_file}: {e}")
        raise

def _sftp_download_dir(sftp, remote_dir, local_dir, dry_run=False):
    """Recursively downloads the contents of a remote directory."""
    if not dry_run:
        Path(local_dir).mkdir(parents=True, exist_ok=True)

    for item in sftp.listdir(remote_dir):
        remote_item_path = os.path.join(remote_dir, item).replace("\\", "/")
        local_item_path = os.path.join(local_dir, item)
        transfer_content(sftp, remote_item_path, local_item_path, dry_run)

def transfer_content(sftp, remote_path, local_path, dry_run=False):
    """
    Transfers a remote file or directory to a local path, preserving structure.
    This acts as a dispatcher to the file/directory-specific functions.
    """
    try:
        remote_stat = sftp.stat(remote_path)
        if remote_stat.st_mode & 0o40000:  # S_ISDIR
            _sftp_download_dir(sftp, remote_path, local_path, dry_run)
        else:
            _sftp_download_file(sftp, remote_path, local_path, dry_run)
    except FileNotFoundError:
        logging.error(f"Remote path not found during SFTP transfer: {remote_path}")
        raise

# --- Torrent Processing Logic ---

def get_eligible_torrents(client, category):
    """Gets a list of completed torrents from the client that match the specified category."""
    try:
        torrents = client.torrents_info(category=category, status_filter='completed')
        logging.info(f"Found {len(torrents)} completed torrent(s) in category '{category}'.")
        return torrents
    except Exception as e:
        logging.error(f"Could not retrieve torrents from client: {e}")
        return []

def wait_for_recheck_completion(client, torrent_hash, timeout_seconds=900, dry_run=False):
    """Waits for a torrent to complete its recheck. Returns True on success."""
    if dry_run:
        logging.info(f"[DRY RUN] Would wait for recheck on {torrent_hash[:10]}. Assuming success.")
        return True

    start_time = time.time()
    logging.info(f"Waiting for recheck to complete for torrent {torrent_hash[:10]}...")
    while time.time() - start_time < timeout_seconds:
        try:
            torrent_info = client.torrents_info(torrent_hashes=torrent_hash)
            if not torrent_info:
                logging.warning(f"Torrent {torrent_hash[:10]} disappeared while waiting for recheck.")
                return False
            torrent = torrent_info[0]
            if torrent.progress == 1:
                logging.info(f"Recheck completed for torrent {torrent_hash[:10]}.")
                return True
            time.sleep(10)
        except Exception as e:
            logging.error(f"Error while waiting for recheck on {torrent_hash[:10]}: {e}")
            return False
    logging.error(f"Timeout: Recheck did not complete for torrent {torrent_hash[:10]} in {timeout_seconds}s.")
    return False

# --- Tracker-based Categorization ---

def load_tracker_rules(script_dir, rules_filename="tracker_rules.json"):
    """
    Loads tracker-to-category rules from the specified JSON file.
    Returns an empty dictionary if the file doesn't exist or is invalid.
    """
    rules_file = script_dir / rules_filename
    if not rules_file.is_file():
        logging.warning(f"Tracker rules file not found at '{rules_file}'. Starting with empty ruleset.")
        return {}
    try:
        with open(rules_file, 'r') as f:
            rules = json.load(f)
        logging.info(f"Successfully loaded {len(rules)} tracker rules from '{rules_file}'.")
        return rules
    except json.JSONDecodeError:
        logging.error(f"Could not decode JSON from '{rules_file}'. Please check its format.")
        return {}

def save_tracker_rules(rules, script_dir, rules_filename="tracker_rules.json"):
    """Saves the tracker rules dictionary to the specified JSON file."""
    rules_file = script_dir / rules_filename
    try:
        with open(rules_file, 'w') as f:
            json.dump(rules, f, indent=4, sort_keys=True)
        logging.info(f"Successfully saved {len(rules)} rules to '{rules_file}'.")
        return True
    except Exception as e:
        logging.error(f"Failed to save rules to '{rules_file}': {e}")
        return False

def get_tracker_domain(tracker_url):
    """Extracts the network location (domain) from a tracker URL."""
    try:
        netloc = urlparse(tracker_url).netloc
        # Simple subdomain stripping for better matching
        parts = netloc.split('.')
        if len(parts) > 2:
            # e.g., tracker.site.com -> site.com, announce.site.org -> site.org
            if parts[0] in ['tracker', 'announce', 'www']:
                return '.'.join(parts[1:])
        return netloc
    except Exception:
        return None

def set_category_based_on_tracker(client, torrent_hash, tracker_rules, dry_run=False):
    """
    Sets a torrent's category based on its trackers and the predefined rules.
    """
    try:
        torrent_info = client.torrents_info(torrent_hashes=torrent_hash)
        if not torrent_info:
            logging.warning(f"Could not find torrent {torrent_hash[:10]} on destination to categorize.")
            return

        torrent = torrent_info[0]
        trackers = client.torrents_trackers(torrent_hash=torrent.hash)
        logging.info(f"Checking {len(trackers)} trackers for '{torrent.name}' for categorization...")

        for tracker in trackers:
            tracker_url = tracker.get('url')
            if not tracker_url:
                continue

            domain = get_tracker_domain(tracker_url)
            logging.debug(f"Processing tracker '{tracker_url}' -> domain '{domain}'")

            if domain and domain in tracker_rules:
                category = tracker_rules[domain]
                if torrent.category == category:
                    logging.info(f"Torrent '{torrent.name}' is already in the correct category '{category}'.")
                    return # Already correct

                logging.info(f"Rule found for tracker '{domain}'. Setting category to '{category}' for '{torrent.name}'.")
                if not dry_run:
                    client.torrents_set_category(torrent_hashes=torrent.hash, category=category)
                else:
                    logging.info(f"[DRY RUN] Would set category of '{torrent.name}' to '{category}'.")
                return # Stop after first match

        logging.info(f"No matching tracker rule found for torrent '{torrent.name}'.")

    except qbittorrentapi.exceptions.NotFound404Error:
        logging.warning(f"Torrent {torrent_hash[:10]} not found on destination when trying to categorize.")
    except Exception as e:
        logging.error(f"An error occurred during categorization for torrent {torrent_hash[:10]}: {e}", exc_info=True)


def process_torrent(torrent, mandarin_qbit, unraid_qbit, sftp, config, tracker_rules, dry_run=False, test_run=False):
    """Executes the full transfer and management process for a single torrent."""
    name, hash = torrent.name, torrent.hash
    logging.info(f"--- Starting processing for torrent: {name} ---")
    try:
        # 1. Pause torrent on Mandarin
        if not dry_run:
            logging.info(f"Pausing torrent on Mandarin: {name}")
            mandarin_qbit.torrents_pause(torrent_hashes=hash)
        else:
            logging.info(f"[DRY RUN] Would pause torrent on Mandarin: {name}")

        # 2. Transfer files via SFTP
        source_base_path = config['MANDARIN_SFTP']['source_path']
        dest_base_path = config['UNRAID_PATHS']['destination_path']
        # Get the remote path for qBittorrent, falling back to the physical destination path
        remote_dest_base_path = config['UNRAID_PATHS'].get('remote_destination_path') or dest_base_path

        remote_content_path = torrent.content_path
        if not remote_content_path.startswith(source_base_path):
            raise ValueError(f"Content path '{remote_content_path}' not inside source path '{source_base_path}'.")
        relative_path = os.path.relpath(remote_content_path, source_base_path)
        local_dest_path = os.path.join(dest_base_path, relative_path)

        logging.info(f"Starting SFTP transfer from '{remote_content_path}' to '{local_dest_path}'")
        transfer_content(sftp, remote_content_path, local_dest_path, dry_run)
        logging.info("SFTP transfer completed successfully.")

        # 3. Add to Unraid, paused
        # The save path for qBit needs to be the directory that will *contain* the torrent's content.
        unraid_save_path = os.path.join(remote_dest_base_path, os.path.dirname(relative_path))
        unraid_save_path = unraid_save_path.replace("\\", "/") # Ensure forward slashes for cross-platform compatibility

        if not dry_run:
            # Export the .torrent file from the source client
            logging.info(f"Exporting .torrent file for {name}")
            torrent_file_content = mandarin_qbit.torrents_export(torrent_hash=hash)

            logging.info(f"Adding torrent to Unraid (paused) with save path '{unraid_save_path}': {name}")
            unraid_qbit.torrents_add(
                torrent_files=torrent_file_content,
                save_path=unraid_save_path,
                is_paused=True,
                category=torrent.category
            )
            time.sleep(5)
        else:
            logging.info(f"[DRY RUN] Would export and add torrent to Unraid (paused) with save path '{unraid_save_path}': {name}")

        # 4. Force recheck on Unraid
        if not dry_run:
            logging.info(f"Triggering force recheck on Unraid for: {name}")
            unraid_qbit.torrents_recheck(torrent_hashes=hash)
        else:
            logging.info(f"[DRY RUN] Would trigger force recheck on Unraid for: {name}")

        # 5. Wait for recheck and then start
        if wait_for_recheck_completion(unraid_qbit, hash, dry_run=dry_run):
            if not dry_run:
                logging.info(f"Starting torrent on Unraid: {name}")
                unraid_qbit.torrents_resume(torrent_hashes=hash)
            else:
                logging.info(f"[DRY RUN] Would start torrent on Unraid: {name}")

            # 6. Set category based on tracker rules
            logging.info(f"Attempting to categorize torrent on Unraid: {name}")
            set_category_based_on_tracker(unraid_qbit, hash, tracker_rules, dry_run=dry_run)

            # 7. Delete from Mandarin
            if test_run:
                logging.info(f"[TEST RUN] Skipping deletion of torrent from Mandarin: {name}")
            elif not dry_run:
                logging.info(f"Deleting torrent and data from Mandarin: {name}")
                mandarin_qbit.torrents_delete(torrent_hashes=hash, delete_files=True)
            else:
                logging.info(f"[DRY RUN] Would delete torrent and data from Mandarin: {name}")

            logging.info(f"--- Successfully processed torrent: {name} ---")
            return True
        else:
            logging.error(f"Failed to verify recheck for {name}. Leaving on Mandarin for next run.")
            return False
    except Exception as e:
        logging.error(f"An error occurred while processing torrent {name}: {e}", exc_info=True)
        if not dry_run:
            try:
                mandarin_qbit.torrents_resume(torrent_hashes=hash)
            except Exception as resume_e:
                logging.error(f"Failed to resume torrent {name} on Mandarin after error: {resume_e}")
        return False

# --- Main Execution ---

def run_interactive_categorization(client, rules, script_dir, category_to_scan):
    """
    Scans torrents in a specific category and interactively prompts the user
    to create categorization rules for torrents that don't have one.
    """
    logging.info(f"Scanning category '{category_to_scan}' for torrents without a known tracker rule...")
    try:
        torrents_in_category = client.torrents_info(category=category_to_scan, sort='name')
        available_categories = sorted(list(client.torrent_categories.categories.keys()))

        if not available_categories:
            logging.error("No categories found on the destination client. Cannot perform categorization.")
            return

        known_domains = set(rules.keys())
        updated_rules = rules.copy()
        rules_changed = False

        for torrent in torrents_in_category:
            trackers = client.torrents_trackers(torrent_hash=torrent.hash)

            # Find the first valid http/https tracker domain that doesn't already have a rule
            domain_to_propose = None
            all_domains = set()
            for tracker in trackers:
                domain = get_tracker_domain(tracker.get('url'))
                if domain:
                    all_domains.add(domain)
                    # Propose a rule if we find a domain that is not already known
                    if domain not in known_domains:
                        domain_to_propose = domain
                        break # Found a candidate, stop checking trackers for this torrent

            if not domain_to_propose:
                # This torrent has no new/unknown valid trackers, so we skip it
                continue

            # If we have a domain to propose, prompt the user
            print("-" * 60)
            print(f"Found torrent with a new tracker domain: {torrent.name}")
            print(f"   Proposing rule for: {domain_to_propose}")
            if len(all_domains) > 1:
                print(f"   All Tracker Domains: {', '.join(sorted(all_domains))}")
            print(f"   Current Category: {torrent.category or 'None'}")

            print(f"\nPlease choose a category for the domain '{domain_to_propose}':")
            for i, cat in enumerate(available_categories):
                print(f"  {i+1}: {cat}")
            print("  s: Skip this torrent")
            print("  q: Quit interactive session")

            while True:
                    choice = input("Enter your choice (1-N, s, q): ").lower()
                    if choice == 'q':
                        if rules_changed:
                            save_tracker_rules(updated_rules, script_dir)
                        return
                    if choice == 's':
                        break

                    try:
                        choice_idx = int(choice) - 1
                        if 0 <= choice_idx < len(available_categories):
                            chosen_category = available_categories[choice_idx]
                            print(f"Applying category '{chosen_category}' and creating rule for '{domain_to_propose}'.")

                            client.torrents_set_category(torrent_hashes=torrent.hash, category=chosen_category)

                            updated_rules[domain_to_propose] = chosen_category
                            known_domains.add(domain_to_propose)
                            rules_changed = True
                            break
                        else:
                            print("Invalid number. Please try again.")
                    except ValueError:
                        print("Invalid input. Please enter a number, 's', or 'q'.")

        if rules_changed:
            save_tracker_rules(updated_rules, script_dir)

        print("-" * 60)
        logging.info("Interactive categorization session finished.")

    except Exception as e:
        logging.error(f"An error occurred during interactive categorization: {e}", exc_info=True)

def main():
    """Main entry point for the script."""
    # The default config file is expected to be in the same directory as the script.
    script_dir = Path(__file__).resolve().parent
    default_config_path = script_dir / 'config.ini'

    parser = argparse.ArgumentParser(
        description="A script to move qBittorrent torrents and data between servers.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--config',
        default=str(default_config_path),
        help='Path to the configuration file.'
    )

    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--dry-run', action='store_true', help='Simulate the process without making any changes.')
    mode_group.add_argument('--test-run', action='store_true', help='Run the full process but do not delete the source torrent.')

    # Rule management arguments
    parser.add_argument('--list-rules', action='store_true', help='List all tracker-to-category rules and exit.')
    parser.add_argument('--add-rule', nargs=2, metavar=('TRACKER_DOMAIN', 'CATEGORY'), help='Add or update a rule and exit.')
    parser.add_argument('--delete-rule', metavar='TRACKER_DOMAIN', help='Delete a rule and exit.')
    parser.add_argument('--interactive-categorize', action='store_true', help='Interactively categorize torrents on destination without a rule.')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    tracker_rules = load_tracker_rules(script_dir)

    # --- Handle Rule Management ---
    if args.list_rules:
        logging.info("--- Current Tracker Rules ---")
        if not tracker_rules:
            print("No rules defined.")
        else:
            for domain, category in sorted(tracker_rules.items()):
                print(f"{domain:<30} -> {category}")
        return 0 # Exit after listing

    if args.add_rule:
        domain, category = args.add_rule
        logging.info(f"Adding/updating rule: '{domain}' -> '{category}'")
        tracker_rules[domain] = category
        save_tracker_rules(tracker_rules, script_dir)
        return 0 # Exit after adding

    if args.delete_rule:
        domain = args.delete_rule
        if domain in tracker_rules:
            logging.info(f"Deleting rule for domain: '{domain}'")
            del tracker_rules[domain]
            save_tracker_rules(tracker_rules, script_dir)
        else:
            logging.error(f"Rule for domain '{domain}' not found.")
            return 1 # Exit with error
        return 0 # Exit after deleting

    # --- Handle Interactive Mode ---
    if args.interactive_categorize:
        logging.info("--- Starting Interactive Categorization Mode ---")
        config = load_config(args.config)
        try:
            category_to_scan = config['SETTINGS']['category_to_move']
        except KeyError:
            logging.error("`category_to_move` not found in [SETTINGS] section of your config. Aborting.")
            return 1

        logging.info(f"Scanning for uncategorized torrents in category: '{category_to_scan}'")
        unraid_qbit = connect_qbit(config['UNRAID_QBIT'], "Unraid")
        if not unraid_qbit:
            logging.error("Could not connect to Unraid qBittorrent. Aborting.")
            return 1

        run_interactive_categorization(unraid_qbit, tracker_rules, script_dir, category_to_scan)
        return 0

    logging.info("--- Torrent Mover script started ---")
    if args.dry_run:
        logging.warning("!!! DRY RUN MODE ENABLED. NO CHANGES WILL BE MADE. !!!")
    if args.test_run:
        logging.warning("!!! TEST RUN MODE ENABLED. SOURCE TORRENTS WILL NOT BE DELETED. !!!")

    config = load_config(args.config)

    mandarin_qbit = connect_qbit(config['MANDARIN_QBIT'], "Mandarin")
    unraid_qbit = connect_qbit(config['UNRAID_QBIT'], "Unraid")
    sftp_client, sftp_transport = connect_sftp(config['MANDARIN_SFTP'])

    if not all([mandarin_qbit, unraid_qbit, sftp_client]):
        logging.error("One or more connections failed. Please check config and logs. Aborting.")
        return 1

    logging.info("All connections established successfully.")

    try:
        category_to_move = config['SETTINGS']['category_to_move']
        eligible_torrents = get_eligible_torrents(mandarin_qbit, category_to_move)

        if not eligible_torrents:
            logging.info("No torrents to move at this time.")
        else:
            processed_count = 0
            total_count = len(eligible_torrents)
            for i, torrent in enumerate(eligible_torrents, 1):
                logging.info(f"Processing torrent {i}/{total_count}...")
                if process_torrent(torrent, mandarin_qbit, unraid_qbit, sftp_client, config, tracker_rules, dry_run=args.dry_run, test_run=args.test_run):
                    processed_count += 1
            logging.info(f"Processing complete. Moved {processed_count}/{total_count} torrent(s).")

    finally:
        if sftp_client:
            sftp_client.close()
        if sftp_transport:
            sftp_transport.close()
            logging.info("SFTP connection closed.")

    logging.info("--- Torrent Mover script finished ---")
    return 0

if __name__ == "__main__":
    sys.exit(main())
