# Glossary

This glossary defines key terms and concepts used within the Torrent Mover project.

## Definitions

*   **Ephemeral Circuit Breaker**
    *   Failure counting resets on script exit. It is a mechanism to stop retrying a failing operation after a certain number of attempts, but the state is not persistent across restarts.

*   **Delta Transfer**
    *   Rsync's ability to only send file differences. This optimizes bandwidth usage by transferring only the changed parts of a file rather than the entire file.

*   **Checkpoint**
    *   Record of successfully completed transfers. This persistent record prevents re-transferring files that have already been successfully moved.

*   **Smart Heartbeat**
    *   Detection of rsync activity during checksum phase. This prevents timeouts during long checksum calculations where no data is being transferred but the process is still active.

*   **Transfer Multiplier**
    *   Factor accounting for cache-based transfers (2x for download+upload). It adjusts the progress estimation or resource allocation when data passes through an intermediate cache.

*   **Corruption Flag**
    *   Marker indicating a file failed integrity checks. This flag signals that the file is corrupted and may require specific handling or re-transfer.
