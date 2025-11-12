"""Manages tracker-based categorization rules for torrents.

This module provides functionality to automatically assign categories to torrents
based on their tracker URLs. It supports loading, saving, and displaying rules,
as well as both automatic and interactive categorization modes.

Key features include:
- Loading tracker-to-category rules from a `tracker_rules.json` file.
- Saving updated rules back to the file.
- Extracting the base domain from a full tracker URL for rule matching.
- Applying categories to torrents based on the first matching tracker rule.
- An interactive command-line tool for users to manually categorize torrents
  and create new rules on the fly.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Optional, List
from urllib.parse import urlparse

import qbittorrentapi
from rich.console import Console
from rich.table import Table


def display_tracker_rules(rules: Dict[str, str]) -> None:
    """Displays the current tracker-to-category rules in a formatted table.

    Args:
        rules: A dictionary where keys are tracker domains and values are the
            categories to assign.
    """
    if not rules:
        logging.info("No tracker-to-category rules are currently defined.")
        return
    console = Console()
    table = Table(title="Tracker to Category Rules", show_header=True, header_style="bold magenta")
    table.add_column("Tracker Domain", style="dim", width=40)
    table.add_column("Assigned Category")
    sorted_rules = sorted(rules.items())
    for domain, category in sorted_rules:
        style = "yellow" if category == "ignore" else "cyan"
        table.add_row(domain, f"[{style}]{category}[/{style}]")
    console.print(table)


def load_tracker_rules(script_dir: Path, rules_filename: str = "tracker_rules.json") -> Dict[str, str]:
    """Loads tracker-to-category mapping rules from a JSON file.

    Args:
        script_dir: The directory where the script is located, used to find the rules file.
        rules_filename: The name of the JSON file containing the rules.

    Returns:
        A dictionary of tracker rules. Returns an empty dictionary if the file
        is not found or contains invalid JSON.
    """
    rules_file = script_dir / rules_filename
    if not rules_file.is_file():
        logging.warning(f"Tracker rules file not found at '{rules_file}'. Starting with an empty ruleset.")
        return {}
    try:
        with open(rules_file, 'r', encoding='utf-8') as f:
            rules = json.load(f)
        logging.info(f"Successfully loaded {len(rules)} tracker rules from '{rules_file}'.")
        return rules
    except json.JSONDecodeError:
        logging.error(f"Could not decode JSON from '{rules_file}'. Please check its format.")
        return {}

def save_tracker_rules(rules: Dict[str, str], script_dir: Path, rules_filename: str = "tracker_rules.json") -> bool:
    """Saves the tracker rules dictionary to a JSON file.

    The file is saved with indentation for readability.

    Args:
        rules: The dictionary of rules to save.
        script_dir: The directory where the script is located.
        rules_filename: The name of the JSON file to save the rules to.

    Returns:
        `True` if the rules were saved successfully, `False` otherwise.
    """
    rules_file = script_dir / rules_filename
    try:
        with open(rules_file, 'w', encoding='utf-8') as f:
            json.dump(rules, f, indent=4, sort_keys=True)
        logging.info(f"Successfully saved {len(rules)} rules to '{rules_file}'.")
        return True
    except IOError as e:
        logging.error(f"Failed to save rules to '{rules_file}': {e}")
        return False

def get_tracker_domain(tracker_url: str) -> Optional[str]:
    """Extracts the effective domain name from a tracker URL.

    This function parses a URL and attempts to find the base domain name for rule
    matching. It strips common subdomains like 'tracker', 'announce', or 'www'.

    Example:
        'http://tracker.example.com:8080/announce' -> 'example.com'

    Args:
        tracker_url: The full URL of the tracker.

    Returns:
        The extracted domain name as a string, or `None` if parsing fails.
    """
    try:
        netloc = urlparse(tracker_url).netloc.split(':')[0]  # Ignore port
        parts = netloc.split('.')
        # Strip common subdomains if there are more than 2 parts
        if len(parts) > 2 and parts[0] in ['tracker', 'announce', 'www']:
            return '.'.join(parts[1:])
        return netloc
    except Exception:
        return None

def get_category_from_rules(torrent: qbittorrentapi.TorrentDictionary, rules: Dict[str, str], client: qbittorrentapi.Client) -> Optional[str]:
    """Finds a matching category for a torrent based on its trackers and the rule set.

    This function iterates through a torrent's trackers, extracts the domain for
    each one, and checks if that domain exists in the provided rules dictionary.
    The first match found determines the category.

    Args:
        torrent: The qBittorrent torrent object to check.
        rules: A dictionary of tracker-to-category rules.
        client: An authenticated qBittorrent client instance to fetch tracker info.

    Returns:
        The matched category name as a string, or `None` if no rule matches.
    """
    try:
        trackers = client.torrents_trackers(torrent_hash=torrent.hash)
        for tracker in trackers:
            domain = get_tracker_domain(tracker.url)
            if domain and domain in rules:
                return rules[domain]
    except Exception as e:
        logging.warning(f"Could not check trackers for torrent '{torrent.name}': {e}")
    return None

def categorize_torrents(client: qbittorrentapi.Client, torrents: List[qbittorrentapi.TorrentDictionary], tracker_rules: Dict[str, str]) -> None:
    """Categorizes a list of torrents based on the provided tracker rules.

    Args:
        client: An authenticated qBittorrent client instance.
        torrents: A list of torrent objects to be categorized.
        tracker_rules: The dictionary of tracker-to-category rules to apply.
    """
    logging.info(f"Starting automatic categorization for {len(torrents)} torrent(s)...")
    for torrent in torrents:
        set_category_based_on_tracker(client, torrent.hash, tracker_rules)
    logging.info("Automatic categorization finished.")


def set_category_based_on_tracker(client: qbittorrentapi.Client, torrent_hash: str, tracker_rules: Dict[str, str], dry_run: bool = False) -> None:
    """Sets a single torrent's category based on matching tracker rules.

    This function fetches a torrent's information, finds a matching category
    from the rules, and then applies that category in the qBittorrent client.
    It will not change the category if it is already correct or if the rule
    specifies "ignore".

    Args:
        client: An authenticated qBittorrent client instance.
        torrent_hash: The hash of the torrent to categorize.
        tracker_rules: The dictionary of tracker-to-category rules.
        dry_run: If `True`, logs the action without making any changes.
    """
    try:
        torrent_list = client.torrents_info(torrent_hashes=torrent_hash)
        if not torrent_list:
            logging.warning(f"Could not find torrent {torrent_hash[:10]} to categorize.")
            return
        torrent = torrent_list[0]
        category = get_category_from_rules(torrent, tracker_rules, client)
        if category:
            if category == "ignore":
                logging.info(f"Rule is to 'ignore' torrent '{torrent.name}'. No action taken.")
                return
            if torrent.category == category:
                logging.debug(f"Torrent '{torrent.name}' is already in the correct category '{category}'.")
                return

            logging.info(f"Rule found: Setting category for '{torrent.name}' to '{category}'.")
            if not dry_run:
                client.torrents_set_category(torrent_hashes=torrent.hash, category=category)
            else:
                logging.info(f"[DRY RUN] Would set category of '{torrent.name}' to '{category}'.")
        else:
            logging.debug(f"No matching tracker rule found for torrent '{torrent.name}'.")
    except qbittorrentapi.exceptions.NotFound404Error:
        logging.warning(f"Torrent {torrent_hash[:10]} not found on client during categorization attempt.")
    except Exception as e:
        logging.error(f"An error occurred during categorization for torrent {torrent_hash[:10]}: {e}", exc_info=True)

def run_interactive_categorization(client: qbittorrentapi.Client, rules: Dict[str, str], script_dir: Path, category_to_scan: str, no_rules: bool = False) -> None:
    """Starts a command-line session to interactively categorize torrents and create rules.

    This function scans a specified category for completed torrents that do not
    match any existing rules (unless `no_rules` is `True`). For each such torrent,
    it presents the user with a list of available categories and prompts them to
    assign one. The user can also choose to create a new rule for the torrent's
    tracker domain, which will be saved for future use.

    Args:
        client: An authenticated qBittorrent client instance.
        rules: The current dictionary of tracker rules, which will be updated by the session.
        script_dir: The directory of the script, used for saving the updated rules file.
        category_to_scan: The category to scan. If an empty string, it scans uncategorized torrents.
        no_rules: If `True`, the session will ignore existing rules and prompt for all
            torrents in the specified category.
    """
    logging.info("Starting interactive categorization...")
    if no_rules:
        logging.warning("Ignoring existing rules for this session (--no-rules).")
    try:
        if not category_to_scan:
            logging.info("No category specified. Scanning 'uncategorized' torrents.")
            all_torrents = client.torrents_info(filter='uncategorized', sort='name')
        else:
            logging.info(f"Scanning for torrents in category: '{category_to_scan}'")
            all_torrents = client.torrents_info(category=category_to_scan, sort='name')

        torrents_to_check = [t for t in all_torrents if t.progress == 1]
        if not torrents_to_check:
            logging.info(f"No completed torrents found in '{category_to_scan or 'uncategorized'}' that need categorization.")
            return

        available_categories = sorted(list(client.torrent_categories.categories.keys()))
        if not available_categories:
            logging.error("No categories found on the client. Cannot perform categorization.")
            return

        updated_rules = rules.copy()
        rules_changed = False
        console = Console()

        for torrent in torrents_to_check:
            # Skip if a rule already exists, unless --no-rules is used
            if not no_rules and get_category_from_rules(torrent, updated_rules, client):
                continue

            console.print("-" * 60)
            console.print(f"Torrent: [bold]{torrent.name}[/bold]")
            trackers = client.torrents_trackers(torrent_hash=torrent.hash)
            torrent_domains = sorted(list(set(d for d in [get_tracker_domain(t.url) for t in trackers] if d)))
            console.print(f"  Domains: {', '.join(torrent_domains) if torrent_domains else 'None'}")

            console.print("\n[bold]Choose an action:[/bold]")
            for i, cat in enumerate(available_categories):
                console.print(f"  {i+1}: Set category to '{cat}'")
            console.print("\n  [cyan]s[/cyan]: Skip | [cyan]i[/cyan]: Ignore permanently | [cyan]q[/cyan]: Quit and Save")

            while True:
                choice = console.input("Enter choice: ").lower().strip()
                if choice == 'q':
                    if rules_changed: save_tracker_rules(updated_rules, script_dir)
                    return
                if choice == 's':
                    break
                if choice == 'i':
                    if torrent_domains:
                        for domain in torrent_domains:
                            if domain not in updated_rules:
                                console.print(f"Creating 'ignore' rule for: {domain}")
                                updated_rules[domain] = "ignore"
                                rules_changed = True
                    break
                try:
                    choice_idx = int(choice) - 1
                    if 0 <= choice_idx < len(available_categories):
                        chosen_category = available_categories[choice_idx]
                        client.torrents_set_category(torrent_hashes=torrent.hash, category=chosen_category)
                        console.print(f"Category set to '{chosen_category}'.")

                        if torrent_domains:
                            learn = console.input("Create a rule for this choice? (y/n): ").lower().strip()
                            if learn == 'y':
                                domain_to_rule = torrent_domains[0]
                                if len(torrent_domains) > 1:
                                    console.print("Multiple domains found. Choose one for the rule:")
                                    for j, domain in enumerate(torrent_domains): console.print(f"  {j+1}: {domain}")
                                    domain_choice = console.input(f"Enter number (1-{len(torrent_domains)}): ")
                                    domain_idx = int(domain_choice) - 1
                                    if 0 <= domain_idx < len(torrent_domains):
                                        domain_to_rule = torrent_domains[domain_idx]

                                console.print(f"Creating rule: '{domain_to_rule}' -> '{chosen_category}'")
                                updated_rules[domain_to_rule] = chosen_category
                                rules_changed = True
                        break
                    else:
                        console.print("[red]Invalid number.[/red]")
                except ValueError:
                    console.print("[red]Invalid input. Please enter a number or a valid command.[/red]")

        if rules_changed:
            save_tracker_rules(updated_rules, script_dir)

        console.print("-" * 60)
        logging.info("Interactive categorization session finished.")
    except Exception as e:
        logging.error(f"An error occurred during interactive categorization: {e}", exc_info=True)
