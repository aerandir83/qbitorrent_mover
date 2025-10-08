import base64
import logging
import keyring
from cryptography.fernet import Fernet, InvalidToken

# --- Constants for Keyring Service ---
# These identify the application to the OS keychain.
KEYRING_SERVICE_NAME = "torrent-mover-script"
KEYRING_USERNAME = "encryption_key"

# The prefix to identify encrypted values in the config file.
ENCRYPTION_PREFIX = "ENC:"

def _get_encryption_key() -> bytes:
    """
    Retrieves the encryption key from the OS keychain.
    If the key does not exist, it generates a new one, stores it,
    and returns it.
    """
    try:
        # Retrieve the key from the keychain.
        key_str = keyring.get_password(KEYRING_SERVICE_NAME, KEYRING_USERNAME)

        if key_str:
            # Key exists, decode it from Base64
            logging.debug("Found existing encryption key in OS keychain.")
            return base64.urlsafe_b64decode(key_str)
        else:
            # Key does not exist, generate a new one.
            logging.info("No encryption key found. Generating a new one and storing it in the OS keychain.")
            new_key = Fernet.generate_key()
            # Store the new key as a Base64 encoded string.
            keyring.set_password(KEYRING_SERVICE_NAME, KEYRING_USERNAME, base64.urlsafe_b64encode(new_key).decode('utf-8'))
            logging.info("A new encryption key has been securely stored.")
            return new_key
    except Exception as e:
        logging.error("FATAL: Could not access the OS keychain.")
        logging.error("Please ensure you have a supported keychain/credential manager installed and configured.")
        logging.error(f"Underlying error: {e}")
        # Keyring can be problematic in some environments (e.g. headless servers without a DBus session).
        # Provide guidance for users.
        logging.error("For headless Linux, you may need to install a DBus session and a supported backend like 'SecretService'.")
        exit(1)

def encrypt_password(password_to_encrypt: str) -> str:
    """
    Encrypts a password using the key from the OS keychain.
    Returns a string containing the encrypted data, prefixed for identification.
    """
    if not password_to_encrypt:
        return ""

    key = _get_encryption_key()
    f = Fernet(key)
    encrypted_data = f.encrypt(password_to_encrypt.encode('utf-8'))
    # Return the encrypted data as a prefixed, base64 string
    return f"{ENCRYPTION_PREFIX}{base64.b64encode(encrypted_data).decode('utf-8')}"

def decrypt_password(encrypted_password_str: str) -> str:
    """
    Decrypts a password using the key from the OS keychain.
    The input is the prefixed, base64-encoded string from the config file.
    """
    if not isinstance(encrypted_password_str, str) or not encrypted_password_str.startswith(ENCRYPTION_PREFIX):
        # If the value is not an encrypted string, return it as is.
        return encrypted_password_str

    try:
        # Remove prefix and decode from base64
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