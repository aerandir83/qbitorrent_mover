# Architecture & System Constraints

> **CRITICAL FOR AI AGENTS:** This document is the **Source of Truth** for system interactions. You MUST read this before refactoring any code. If you change how modules interact, you MUST update this file.

## 1. Dependency Graph & Ownership

The system follows a strict hierarchical Manager pattern. Circular dependencies are FORBIDDEN.

* **`TorrentMover` (Root Orchestrator):**
    * Initializes all Managers.
    * Holds the main execution loop, **Discovery**, and **Filtering** logic.
    * **Responsibility:** Decides *what* to transfer and *when* (Scheduling).
    * **Constraints:** Never contains low-level transfer logic. Delegates execution to `TransferManager`.

* **`TransferManager` (Core Execution):**
    * **Dependencies:** `SSHManager`, `QbittorrentManager`, `SystemManager`, `TransferStrategies`.
    * **Responsibility:** Executes the transfer strategy for a specific torrent. Manages `FileTransferTracker` (Checkpointing).
    * **Constraints:** Does not decide policy (which torrents to pick); only executes the transfer of given torrents.

* **`SSHManager` (Infrastructure):**
    * **Responsibility:** Wraps `paramiko` and `subprocess` (rsync). Manages the `SSHConnectionPool`.
    * **Constraints:** No other module is allowed to create raw SSH connections. All network I/O to the remote server goes through here.

* **`QbittorrentManager` (API Gateway):**
    * **Responsibility:** Wraps `qbittorrent-api`.
    * **Constraints:** Returns generic objects/dicts, decoupled from the specific API library versions where possible.

* **`SystemManager` (OS/State):**
    * **Responsibility:** Logging, PID Lockfiles, and system health checks.
    * **Constraints:** Does not handle torrent-specific state (like checkpoints); handles app-wide state (locks/logs).

* **`UI` (Presentation):**
    * **Responsibility:** `UIManagerV2` (Rich) or `SimpleUIManager`.
    * **Constraints:** Thread-safe. MUST be updated via method calls, never by printing to `stdout` from worker threads.

## 2. "Life of a Torrent" (Data Flow)

Refactoring MUST preserve this sequence.

0.  **Pre-flight Checks:** `TorrentMover` verifies client connectivity and destination disk space before starting the UI.
1.  **Discovery:** `QbittorrentManager` fetches torrents completed > `min_seed_time`.
2.  **Filtering:** `TorrentMover` excludes torrents based on `transfer_checkpoint.json` (already moved) AND Tracker Rules (ignored domains).
3.  **Categorization:** `TrackerManager` applies rules to assign a category (modifying the torrent metadata in memory).
4.  **Strategy Selection:** `TransferStrategies` selects the mode (`sftp`, `rsync`, `sftp_upload`).
5.  **Execution:** `TransferManager` executes the strategy.
    * *Locking:* `SSHManager` acquires a slot from the pool.
    * *Transfer:* Bytes move.
6.  **Verification:** `QbittorrentManager` triggers "Force Recheck" on destination.
7.  **Finalization:**
    * Success: `TransferManager` (via `FileTransferTracker`) writes to `transfer_checkpoint.json`. Source torrent deleted (if config enabled).
    * Failure: `ResilientQueue` updates failure count (Circuit Breaker).

## 2.1. Transfer Execution Flow

This diagram illustrates the call stack and callback mechanisms during a transfer operation.

```text
+------------------+            +--------------------+            +------------------+
| torrent_mover.py |            | transfer_manager.py|            | process_runner.py|
+------------------+            +--------------------+            +------------------+
         |                                |                                |
         | calls transfer strategy        |                                |
         | (e.g. transfer_content_rsync)  |                                |
         |------------------------------->|                                |
         |                                |                                |
         |                                | execute_streaming_command()    |
         |                                |------------------------------->|
         |                                |                                |
         |                                |                                |--- (Subprocess: rsync/ssh)
         |                                |                                |
         |                                |                                |<-- stdout parsing
         |                                |                                |
         | _handle_transfer_log()         |                                |
         |<----------------------------------------------------------------|
         |                                |                                |
         | _update_transfer_progress()    |                                |
         |<----------------------------------------------------------------|
         |                                |                                |
         |                                | returns success/fail           |
         |                                |<-------------------------------|
         |                                |                                |
         | returns success/fail           |                                |
         |<-------------------------------|                                |
```

## 3. State Persistence

* **`transfer_checkpoint.json`**: Stores hashes of successfully moved torrents.
    * *Owner:* `TransferManager` (managed by `FileTransferTracker` class).
* **`ResilientQueue` State**: In-memory only (Ephemeral).
    * *Owner:* `ResilientQueue` (Reset on restart).
* **`config.ini`**: Read-only during runtime.
    * *Owner:* `ConfigManager`.

## 4. Concurrency Model

* **Threading:** `ThreadPoolExecutor` is used for concurrent transfers.
* **Safety:**
    * `SSHConnectionPool` is thread-safe.
    * `UIManagerV2` is thread-safe.
    * **Rule:** Do not share mutable state objects (like lists of torrents) across threads without explicit locking.

## 5. Component Contracts

Defines strict input/output boundaries and guarantees.

* **`TransferManager`**
    * **Input:** `Torrent` object + `TransferStrategy`.
    * **Output:** Success/Failure boolean.
    * **Guarantee:** Atomic file movement or rollback (no partial corrupt files left at destination).

* **`FileTransferTracker`**
    * **Input:** File chunks / Byte progress.
    * **Output:** `file_transfer_tracker.json` (internal state).
    * **Guarantee:** Persists bytes transferred to allow resume support (if strategy permits).

## 6. Visual Reference

### Retry Flow (Ephemeral Circuit Breaker)

```text
     +-----------+        (Fail)         +----------------------------------+
     |  Attempt  | --------------------> | Increment Counter (RAM/Ephemeral)|
     +-----------+                       +----------------------------------+
           ^                                              |
           |                                              v
     +-----------+                             +-------------------+
     |   Retry   | <-------------------------- |      Backoff      |
     +-----------+                             +-------------------+
```

### System State Machine

```text
[Discovery]
    |
    v
[Filtering] (Check: transfer_checkpoint.json)
    |
    v
[Categorization] (Tracker Rule > Source Category)
    |
    v
[Transfer] (Strategy: RSYNC/SFTP)
    |
    v
[Verification] (Force Recheck)
    |
    v
[Finalization] (Write Checkpoint -> Delete Source)
```
