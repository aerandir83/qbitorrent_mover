import base64
import getpass
import logging
import os
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend

# It's recommended to use a salt, and to store it with the encrypted data.
# 16 bytes is a good size for a salt.
SALT_SIZE = 16
# The prefix to identify encrypted values in the config file.
ENCRYPTION_PREFIX = "ENC:"

def derive_key(password: str, salt: bytes) -> bytes:
    """
    Derives a cryptographic key from a password and salt using PBKDF2.
    """
    if not password:
        raise ValueError("Password cannot be empty.")
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
        backend=default_backend()
    )
    return base64.urlsafe_b64encode(kdf.derive(password.encode()))

def encrypt_password(password_to_encrypt: str, master_password: str) -> str:
    """
    Encrypts a password using a master password.
    Returns a string containing the salt and the encrypted data, prefixed.
    """
    if not password_to_encrypt:
        return ""
    salt = os.urandom(SALT_SIZE)
    key = derive_key(master_password, salt)
    f = Fernet(key)
    encrypted_data = f.encrypt(password_to_encrypt.encode())
    # Prepend the salt to the encrypted data and encode it for safe storage.
    # Also add a prefix to easily identify encrypted values.
    return f"{ENCRYPTION_PREFIX}{base64.b64encode(salt + encrypted_data).decode('utf-8')}"

def decrypt_password(encrypted_password_str: str, master_password: str) -> str:
    """
    Decrypts a password using the master password.
    The input is the prefixed, base64-encoded string from the config file.
    """
    if not encrypted_password_str.startswith(ENCRYPTION_PREFIX):
        # If the value is not encrypted, return it as is.
        return encrypted_password_str

    try:
        # Remove prefix and decode from base64
        encoded_data = encrypted_password_str[len(ENCRYPTION_PREFIX):]
        decoded_data = base64.b64decode(encoded_data)

        # Extract the salt and the encrypted data
        salt = decoded_data[:SALT_SIZE]
        encrypted_data = decoded_data[SALT_SIZE:]

        key = derive_key(master_password, salt)
        f = Fernet(key)
        decrypted_data = f.decrypt(encrypted_data)
        return decrypted_data.decode('utf-8')
    except InvalidToken:
        logging.error("Decryption failed! The master password may be incorrect or the data is corrupt.")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred during decryption: {e}")
        raise

def get_master_password(prompt="Enter master password: "):
    """
    Securely prompts the user for a master password.
    """
    try:
        password = getpass.getpass(prompt)
        if not password:
            logging.warning("Master password cannot be empty.")
            return get_master_password(prompt)
        return password
    except (EOFError, KeyboardInterrupt):
        print() # Add a newline after the prompt
        logging.fatal("Cancelled by user. Aborting.")
        exit(1)