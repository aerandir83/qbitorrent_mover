AGENTS.md
This document provides guidance for AI agents and human contributors working on the Torrent Mover codebase.
🚨 Critical Protocols (READ FIRST)
Before you write a single line of code, you MUST read ARCHITECTURE.md. It is the Source of Truth for dependency management and system boundaries. Violating the constraints in that file will result in broken builds and logical regressions.
1. Mandatory Testing & Verification
This project uses unittest and pytest.
Mandatory Testing: You MUST run python -m unittest discover tests or pytest before starting work to establish a baseline, and after your changes to verify no regressions.Operational Verification: Use --dry-run and --test-permissions to verify logic without destructive actions.
2. Pre-Commit Checklist
You are required to follow the steps in pre_commit_instructions.txt before submitting any changes. This includes:
Verifying architecture compliance.Running the test suite.Checking config template consistency.Updating version numbers.Performing a dry run.
Project Structure & Philosophy
Manager Pattern: The system is composed of hierarchical Managers (SSHManager, TransferManager, QbittorrentManager) orchestrated by TorrentMover.Configuration: All user-facing config resides in config.ini. If you add a parameter, you must add it to config.ini.template.State Management:transfer_checkpoint.json: Tracks successful transfers.queue_state.pkl: Tracks circuit breaker state (failures).
Coding Standards
Python Version: 3.10+Type Hinting: Use standard Python type hints.Logging: Use the SystemManager logger (or standard logging), do not use print() statements for operational logs.
Common Tasks
Refactoring: Always check ARCHITECTURE.md to ensure you aren't creating circular dependencies or violating ownership rules.New Features: Ensure any new logic is placed in the appropriate Manager. Do not bloat TorrentMover with low-level execution details.
