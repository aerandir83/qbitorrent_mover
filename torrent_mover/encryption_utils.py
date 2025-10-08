import base64
import logging
import sys
import keyring
from keyring.backends.fail import NoKeyring
from cryptography.fernet import Fernet, InvalidToken

# --- Constants for Keyring Service ---
KEYRING_SERVICE_NAME = "torrent-mover-script"
KEYRING_USERNAME = "encryption_key"
ENCRYPTION_PREFIX = "ENC:"

def check_keyring_backend():
    """
    Checks if a supported keyring backend is available.
    If not, prints detailed instructions and exits.
    """
    if isinstance(keyring.get_keyring(), NoKeyring):
        logging.error("--- Action Required: No OS Keychain Backend Found ---")
        logging.error("This script uses the OS keychain to securely store passwords, but no supported backend was found on your system.")
        logging.error("To fix this on a headless Debian/Ubuntu server, you can install 'gnome-keyring' and 'dbus'.")
        logging.error("\nRun the following commands:")
        logging.error("sudo apt-get update")
        logging.error("sudo apt-get install -y gnome-keyring dbus-x11")
        logging.error("\nAfter installation, you need to start a DBus session before running the script.")
        logging.error("You can do this by wrapping the script execution with 'dbus-run-session':")
        logging.error("dbus-run-session python3 /path/to/your/torrent_mover.py")
        logging.error("\nFor more details, see: https://pypi.org/project/keyring/")
        sys.exit(1)
    logging.info("Supported OS keychain backend found.")

def _get_encryption_key() -> bytes:
    """
    Retrieves the encryption key from the OS keychain.
    If the key does not exist, it generates a new one, stores it, and returns it.
    """
    try:
        key_str = keyring.get_password(KEYRING_SERVICE_NAME, KEYRING_USERNAME)
        if key_str:
            logging.debug("Found existing encryption key in OS keychain.")
            return base64.urlsafe_b64decode(key_str)
        else:
            logging.info("No encryption key found. Generating a new one and storing it in the OS keychain.")
            new_key = Fernet.generate_key()
            keyring.set_password(KEYRING_SERVICE_NAME, KEYRING_USERNAME, base64.urlsafe_b64encode(new_key).decode('utf-8'))
            logging.info("A new encryption key has been securely stored.")
            return new_key
    except Exception as e:
        logging.error(f"FATAL: An unexpected error occurred while accessing the OS keychain: {e}")
        sys.exit(1)

def encrypt_password(password_to_encrypt: str) -> str:
    """
    Encrypts a password using the key from the OS keychain.
    """
    if not password_to_encrypt:
        return ""
    key = _get_encryption_key()
    f = Fernet(key)
    encrypted_data = f.encrypt(password_to_encrypt.encode('utf-8'))
    return f"{ENCRYPTION_PREFIX}{base64.b64encode(encrypted_data).decode('utf-8')}"

def decrypt_password(encrypted_password_str: str) -> str:
    """
    Decrypts a password using the key from the OS keychain.
    """
    if not isinstance(encrypted_password_str, str) or not encrypted_password_str.startswith(ENCRYPTION_PREFIX):
        return encrypted_password_str
    try:
        encoded_data = encrypted_password_str[len(ENCRYPTION_PREFIX):]
        encrypted_data = base64.b64decode(encoded_data)
        key = _get_encryption_key()
        f = Fernet(key)
        decrypted_data = f.decrypt(encrypted_data)
        return decrypted_data.decode('utf-8')
    except InvalidToken:
        logging.error("Decryption failed! The data may be corrupt or the encryption key has changed.")
        logging.error("This can happen if you moved the config file from another system.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during decryption: {e}")
        raise