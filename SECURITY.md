# Security Policy

## Development Sandboxing

To ensure safe development and testing, this project utilizes a sandboxing environment variable protocol.

The `MOVER_SANDBOX` environment variable defines the execution mode of the application.

*   **When `MOVER_SANDBOX` is set to `true`**:
    *   The application **must** operate in isolated execution mode.
    *   No external writes are permitted.
    *   The application runs in a strict "dry-run only" state, preventing any modification to the production environment or external systems.

## Secret Management

Proper management of sensitive keys is mandatory for all forks and CI environments to prevent data leaks and unauthorized access.

*   **Never Hardcode Secrets**: Sensitive information such as passwords, API keys, and private keys must **never** be hardcoded directly into the source code.
*   **Loading Mechanisms**: Secrets should be loaded via environment variables or secure configuration files (e.g., `config.ini`).
*   **Specific Keys**:
    *   **`QBIT_PASSWORD`**: Use environment variables or the configuration file to supply the qBittorrent password.
    *   **`SSH_KEY`**: Private SSH keys must be managed securely and referenced via configuration, not embedded.

Ensure these secrets are properly encrypted or masked in CI/CD logs and storage.
