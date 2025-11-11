import json
import logging
from pathlib import Path
from typing import Dict, Optional
from urllib.parse import urlparse

import qbittorrentapi
from rich.console import Console
from rich.table import Table


def display_tracker_rules(rules: Dict[str, str]) -> None:
    """Displays the current tracker-to-category rules in a formatted table.

    Args:
        rules: A dictionary where keys are tracker domains and values are
            the categories to assign.
    """
    if not rules:
        logging.info("No rules found.")
        return
    console = Console()
    table = Table(title="Tracker to Category Rules", show_header=True, header_style="bold magenta")
    table.add_column("Tracker Domain", style="dim", width=40)
    table.add_column("Assigned Category")
    sorted_rules = sorted(rules.items())
    for domain, category in sorted_rules:
        table.add_row(domain, f"[yellow]{category}[/yellow]" if category == "ignore" else f"[cyan]{category}[/cyan]")
    console.print(table)


def load_tracker_rules(script_dir: Path, rules_filename: str = "tracker_rules.json") -> Dict[str, str]:
    """Loads tracker-to-category mapping rules from a JSON file.

    Args:
        script_dir: The directory where the script is located, used to find
            the rules file.
        rules_filename: The name of the JSON file containing the rules.

    Returns:
        A dictionary of tracker rules. Returns an empty dictionary if the file
        is not found or contains invalid JSON.
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

def save_tracker_rules(rules: Dict[str, str], script_dir: Path, rules_filename: str = "tracker_rules.json") -> bool:
    """Saves the tracker rules to a JSON file.

    Args:
        rules: The dictionary of rules to save.
        script_dir: The directory where the script is located, used to find
            the rules file.
        rules_filename: The name of the JSON file to save the rules to.

    Returns:
        True if the rules were saved successfully, False otherwise.
    """
    rules_file = script_dir / rules_filename
    try:
        with open(rules_file, 'w') as f:
            json.dump(rules, f, indent=4, sort_keys=True)
        logging.info(f"Successfully saved {len(rules)} rules to '{rules_file}'.")
        return True
    except Exception as e:
        logging.error(f"Failed to save rules to '{rules_file}': {e}")
        return False

def get_tracker_domain(tracker_url: str) -> Optional[str]:
    """Extracts the effective domain name from a tracker URL.

    This function parses a URL and attempts to strip common subdomains like
    'tracker', 'announce', or 'www' to find the base domain name used for
    rule matching.

    Args:
        tracker_url: The full URL of the tracker.

    Returns:
        The extracted domain name as a string, or None if parsing fails.
    """
    try:
        netloc = urlparse(tracker_url).netloc
        parts = netloc.split('.')
        if len(parts) > 2:
            if parts[0] in ['tracker', 'announce', 'www']:
                return '.'.join(parts[1:])
        return netloc
    except Exception:
        return None

def get_category_from_rules(torrent: qbittorrentapi.TorrentDictionary, rules: Dict[str, str], client: qbittorrentapi.Client) -> Optional[str]:
    """Finds a matching category for a torrent based on its trackers.

    This function iterates through a torrent's trackers, extracts the domain
    for each, and checks if that domain exists in the provided rules dictionary.
    The first match found determines the category.

    Args:
        torrent: The torrent object to check.
        rules: A dictionary of tracker-to-category rules.
        client: An authenticated qBittorrent client instance to fetch trackers.

    Returns:
        The matched category as a string, or None if no rule matches.
    """
    try:
        trackers = client.torrents_trackers(torrent_hash=torrent.hash)
        for tracker in trackers:
            domain = get_tracker_domain(tracker.get('url'))
            if domain and domain in rules:
                return rules[domain]
    except Exception as e:
        logging.warning(f"Could not check trackers for torrent '{torrent.name}': {e}")
    return None

def categorize_torrents(client: qbittorrentapi.Client, torrents: "qbittorrentapi.TorrentList", tracker_rules: Dict[str, str]) -> None:
    """
    Categorizes a list of torrents based on tracker rules.

    Args:
        client: An authenticated qBittorrent client instance.
        torrents: A list of torrent objects to be categorized.
        tracker_rules: The dictionary of tracker-to-category rules.
    """
    logging.info(f"Starting categorization for {len(torrents)} torrent(s)...")
    for torrent in torrents:
        set_category_based_on_tracker(client, torrent.hash, tracker_rules)
    logging.info("Categorization finished.")


def set_category_based_on_tracker(client: qbittorrentapi.Client, torrent_hash: str, tracker_rules: Dict[str, str], dry_run: bool = False) -> None:
    """Sets a torrent's category based on matching tracker rules.

    This function fetches a torrent's information, finds a matching category
    using `get_category_from_rules`, and then sets the torrent's category
    on the qBittorrent client if a rule is found and the category is not
    "ignore".

    Args:
        client: An authenticated qBittorrent client instance.
        torrent_hash: The hash of the torrent to categorize.
        tracker_rules: The dictionary of tracker-to-category rules.
        dry_run: If True, logs the action without actually changing the category.
    """
    try:
        torrent_info = client.torrents_info(torrent_hashes=torrent_hash)
        if not torrent_info:
            logging.warning(f"Could not find torrent {torrent_hash[:10]} on destination to categorize.")
            return
        torrent = torrent_info[0]
        category = get_category_from_rules(torrent, tracker_rules, client)
        if category:
            if category == "ignore":
                logging.info(f"Rule is to ignore torrent '{torrent.name}'. Doing nothing.")
                return
            if torrent.category == category:
                logging.info(f"Torrent '{torrent.name}' is already in the correct category '{category}'.")
                return
            logging.info(f"Rule found. Setting category to '{category}' for '{torrent.name}'.")
            if not dry_run:
                client.torrents_set_category(torrent_hashes=torrent.hash, category=category)
            else:
                logging.info(f"[DRY RUN] Would set category of '{torrent.name}' to '{category}'.")
        else:
            logging.info(f"No matching tracker rule found for torrent '{torrent.name}'.")
    except qbittorrentapi.exceptions.NotFound404Error:
        logging.warning(f"Torrent {torrent_hash[:10]} not found on destination when trying to categorize.")
    except Exception as e:
        logging.error(f"An error occurred during categorization for torrent {torrent_hash[:10]}: {e}", exc_info=True)

def run_interactive_categorization(client: qbittorrentapi.Client, rules: Dict[str, str], script_dir: Path, category_to_scan: str, no_rules: bool = False) -> None:
    """Starts a user-interactive session to categorize torrents.

    This function scans a specified category (or uncategorized torrents) for
    torrents that do not match any existing tracker rules. For each such torrent,
    it prompts the user to assign a category and optionally create a new rule
    for the torrent's tracker domain.

    Args:
        client: An authenticated qBittorrent client instance.
        rules: The current dictionary of tracker rules, which may be updated.
        script_dir: The directory of the script, used for saving updated rules.
        category_to_scan: The category to scan for torrents. If empty, scans
            for uncategorized torrents.
        no_rules: If True, the session will ignore existing rules and prompt
            for all torrents in the category.
    """
    logging.info("Starting interactive categorization...")
    if no_rules:
        logging.warning("Ignoring existing rules for this session (--no-rules).")
    try:
        if not category_to_scan:
            logging.info("No category specified. Scanning for 'uncategorized' torrents.")
            torrents_to_check = client.torrents_info(filter='uncategorized', sort='name')
        else:
            logging.info(f"Scanning for torrents in category: '{category_to_scan}'")
            torrents_to_check = client.torrents_info(category=category_to_scan, sort='name')
        if not torrents_to_check:
            logging.info(f"No torrents found in '{category_to_scan or 'uncategorized'}' that need categorization.")
            return
        available_categories = sorted(list(client.torrent_categories.categories.keys()))
        if not available_categories:
            logging.error("No categories found on the destination client. Cannot perform categorization.")
            return
        updated_rules = rules.copy()
        rules_changed = False
        console = Console()
        manual_review_count = 0
        for torrent in torrents_to_check:
            auto_category = None
            if not no_rules:
                auto_category = get_category_from_rules(torrent, updated_rules, client)
            if auto_category:
                if auto_category == "ignore":
                    logging.info(f"Ignoring '{torrent.name}' based on existing 'ignore' rule.")
                elif torrent.category != auto_category:
                    logging.info(f"Rule found for '{torrent.name}'. Setting category to '{auto_category}'.")
                    try:
                        client.torrents_set_category(torrent_hashes=torrent.hash, category=auto_category)
                    except Exception as e:
                        logging.error(f"Failed to set category for '{torrent.name}': {e}", exc_info=True)
                continue
            if manual_review_count == 0:
                logging.info("Some torrents require manual review.")
            manual_review_count += 1
            console.print("-" * 60)
            console.print(f"Torrent needs categorization: [bold]{torrent.name}[/bold]")
            console.print(f"   Current Category: {torrent.category or 'None'}")
            trackers = client.torrents_trackers(torrent_hash=torrent.hash)
            torrent_domains = sorted(list(set(d for d in [get_tracker_domain(t.get('url')) for t in trackers] if d)))
            console.print(f"   Tracker Domains: {', '.join(torrent_domains) if torrent_domains else 'None found'}")
            console.print("\nPlease choose an action:")
            for i, cat in enumerate(available_categories):
                console.print(f"  {i+1}: Set category to '{cat}'")
            console.print("\n  s: Skip this torrent (no changes)")
            console.print("  i: Ignore this torrent's trackers permanently")
            console.print("  q: Quit interactive session")
            while True:
                choice = console.input("Enter your choice: ").lower()
                if choice == 'q':
                    if rules_changed:
                        save_tracker_rules(updated_rules, script_dir)
                    return
                if choice == 's':
                    break
                if choice == 'i':
                    if not torrent_domains:
                        console.print("No domains to ignore. Skipping.")
                        break
                    for domain in torrent_domains:
                        if domain not in updated_rules:
                            console.print(f"Creating 'ignore' rule for domain: {domain}")
                            updated_rules[domain] = "ignore"
                            rules_changed = True
                    break
                try:
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(available_categories):
                        chosen_category = available_categories[choice_idx]
                        console.print(f"Setting category to '{chosen_category}'.")
                        client.torrents_set_category(torrent_hashes=torrent.hash, category=chosen_category)
                        if torrent_domains:
                            while True:
                                learn = console.input("Create a rule for this choice? (y/n): ").lower()
                                if learn in ['y', 'yes']:
                                    if len(torrent_domains) == 1:
                                        domain_to_rule = torrent_domains[0]
                                        console.print(f"Creating rule: '{domain_to_rule}' -> '{chosen_category}'")
                                        updated_rules[domain_to_rule] = chosen_category
                                        rules_changed = True
                                        break
                                    else:
                                        console.print("Choose a domain for the rule:")
                                        for j, domain in enumerate(torrent_domains):
                                            console.print(f"  {j+1}: {domain}")
                                        domain_choice = console.input(f"Enter number (1-{len(torrent_domains)}): ")
                                        try:
                                            domain_idx = int(domain_choice) - 1
                                            if 0 <= domain_idx < len(torrent_domains):
                                                domain_to_rule = torrent_domains[domain_idx]
                                                console.print(f"Creating rule: '{domain_to_rule}' -> '{chosen_category}'")
                                                updated_rules[domain_to_rule] = chosen_category
                                                rules_changed = True
                                                break
                                            else:
                                                console.print("Invalid number.")
                                        except ValueError:
                                            console.print("Invalid input.")
                                elif learn in ['n', 'no']:
                                    break
                        break
                    else:
                        console.print("Invalid number. Please try again.")
                except ValueError:
                    console.print("Invalid input. Please enter a number or a valid command (s, i, q).")
        if rules_changed:
            save_tracker_rules(updated_rules, script_dir)
        console.print("-" * 60)
        logging.info("Interactive categorization session finished.")
    except Exception as e:
        logging.error(f"An error occurred during interactive categorization: {e}", exc_info=True)
