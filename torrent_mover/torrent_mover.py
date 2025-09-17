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
import time
import argparse

# --- SFTP Transfer Logic ---

def _sftp_download_file(sftp, remote_file, local_file, dry_run=False):
    """Downloads a single file, performing the size check for overwriting."""
    local_path = Path(local_file)
    remote_stat = sftp.stat(remote_file)

    if local_path.exists():
        local_size = local_path.stat().st_size
        if local_size == remote_stat.st_size:
            logging.info(f"Skipping file (exists and size matches): {local_path}")
            return
        else:
            logging.warning(f"Overwriting file (size mismatch remote:{remote_stat.st_size} vs local:{local_size}): {local_path}")

    if dry_run:
        logging.info(f"[DRY RUN] Would download: {remote_file} -> {local_path}")
        return

    local_path.parent.mkdir(parents=True, exist_ok=True)
    logging.info(f"Downloading: {remote_file} -> {local_path}")
    sftp.get(remote_file, str(local_path))

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

def process_torrent(torrent, mandarin_qbit, unraid_qbit, sftp, config, dry_run=False):
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
        if not dry_run:
            logging.info(f"Adding torrent to Unraid (paused) with save path '{remote_dest_base_path}': {name}")
            unraid_qbit.torrents_add(
                urls=torrent.magnet_uri,
                save_path=remote_dest_base_path,
                is_paused=True,
                category=torrent.category
            )
            time.sleep(5)
        else:
            logging.info(f"[DRY RUN] Would add torrent to Unraid (paused) with save path '{remote_dest_base_path}': {name}")

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

            # 6. Delete from Mandarin
            if not dry_run:
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

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="A script to move qBittorrent torrents and data between servers.")
    parser.add_argument('--config', default='config.ini', help='Path to the configuration file (default: config.ini)')
    parser.add_argument('--dry-run', action='store_true', help='Simulate the process without making any changes.')
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info("--- Torrent Mover script started ---")
    if args.dry_run:
        logging.warning("!!! DRY RUN MODE ENABLED. NO CHANGES WILL BE MADE. !!!")

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
                if process_torrent(torrent, mandarin_qbit, unraid_qbit, sftp_client, config, dry_run=args.dry_run):
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
