Architecture & System Constraints
CRITICAL FOR AI AGENTS: This document is the Source of Truth for system interactions. You MUST read this before refactoring any code. If you change how modules interact, you MUST update this file.
1. Dependency Graph & Ownership
The system follows a strict hierarchical Manager pattern. Circular dependencies are FORBIDDEN.
TorrentMover (Root Orchestrator)
Responsibility: Decides what to transfer and when (Scheduling). Initializes all Managers. Holds the main execution loop, Discovery, and Filtering logic.Constraints: Never contains low-level transfer logic. Delegates execution to TransferManager.
TransferManager (Core Execution)
Dependencies: SSHManager, QbittorrentManager, SystemManager, TransferStrategies.Responsibility: Executes the transfer strategy for a specific torrent. Manages FileTransferTracker (Checkpointing).Constraints: Does not decide policy (which torrents to pick); only executes the transfer of given torrents.
SSHManager (Infrastructure)
Responsibility: Wraps paramiko and subprocess (rsync). Manages the SSHConnectionPool.Constraints: No other module is allowed to create raw SSH connections. All network I/O to the remote server goes through here.
QbittorrentManager (API Gateway)
Responsibility: Wraps qbittorrent-api.Constraints: Returns generic objects/dicts, decoupled from the specific API library versions where possible.
SystemManager (OS/State)
Responsibility: Logging, PID Lockfiles, and system health checks.Constraints: Does not handle torrent-specific state (like checkpoints); handles app-wide state (locks/logs).
UI (Presentation)
Responsibility: UIManagerV2 (Rich) or SimpleUIManager.Constraints: Thread-safe. MUST be updated via method calls, never by printing to stdout from worker threads.
2. "Life of a Torrent" (Data Flow)
Refactoring MUST preserve this sequence.
Discovery: QbittorrentManager fetches torrents completed > min_seed_time.Filtering: TorrentMover compares torrents against transfer_checkpoint.json (loaded via TransferManager/FileTransferTracker) to skip already-moved items.Categorization: TrackerManager applies rules to assign a category (modifying the torrent metadata in memory).Strategy Selection: TransferStrategies selects the mode (sftp, rsync, sftp_upload).Execution: TransferManager executes the strategy.Locking: SSHManager acquires a slot from the pool.Transfer: Bytes move.Verification: QbittorrentManager triggers "Force Recheck" on destination.Finalization:Success: TransferManager (via FileTransferTracker) writes to transfer_checkpoint.json. Source torrent deleted (if config enabled).Failure: ResilientQueue updates failure count (Circuit Breaker).
3. State Persistence
transfer_checkpoint.json: Stores hashes of successfully moved torrents.Owner: TransferManager (managed by FileTransferTracker class).queue_state.pkl (Circuit Breaker): Stores failure counts and backoff timers.Owner: ResilientQueue.config.ini: Read-only during runtime.Owner: ConfigManager.
4. Concurrency Model
Threading: ThreadPoolExecutor is used for concurrent transfers.Safety:SSHConnectionPool is thread-safe.UIManagerV2 is thread-safe.Rule: Do not share mutable state objects (like lists of torrents) across threads without explicit locking.
