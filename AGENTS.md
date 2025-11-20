# **AGENTS.md: Contributor Guidance**

This document provides essential guidance for both AI agents and human contributors working on the **Torrent Mover** codebase.

## **🚨 Critical Protocols (READ FIRST)**

The architectural document is the single source of truth for the project.

* **Source of Truth:** Before you write a single line of code, you **MUST** read **ARCHITECTURE.md**.  
* **Purpose:** It defines all dependency management rules and system boundaries.  
* **Consequence:** Violating the constraints in that file will result in broken builds and logical regressions.

## **Mandatory Testing & Verification**

This project uses the unittest and pytest frameworks. Testing and operational verification are required for all changes.

### **Mandatory Testing**

You must verify the stability of the system both before and after making changes.

1. **Establish Baseline:** Run the test suite **before** starting work.  
2. **Verify No Regressions:** Run the test suite **after** your changes.  
3. **Test Commands:** Use either of the following commands:  
   python \-m unittest discover tests  
   \# OR  
   pytest

### **Operational Verification**

* Use the dedicated flags to verify logic without performing destructive actions.  
* **Verification Flags:** Use **\--dry-run** and **\--test-permissions**.

## **Pre-Commit Checklist**

You are **required** to follow the steps outlined in **pre\_commit\_instructions.txt** before submitting any changes. This checklist includes:

1. Verifying compliance against **ARCHITECTURE.md**.  
2. Running the test suite to ensure no regressions.  
3. Checking config template consistency (ensuring all new parameters are in **config.ini.template**).  
4. Updating version numbers (if applicable).  
5. Performing a final operational dry run (**\--dry-run**).

## **Project Structure & Philosophy**

### **Manager Pattern**

The system architecture is based on hierarchical Managers orchestrated by the main application object.

* **Orchestrator:** **TorrentMover** handles the high-level orchestration.  
* **Managers:** Low-level logic and execution details are delegated to dedicated classes (e.g., SSHManager, TransferManager, QbittorrentManager).

### **Configuration**

* **User Config:** All user-facing configuration resides in **config.ini**.  
* **Template Requirement:** If you add a user-facing parameter, you **must** also add it to **config.ini.template**.

### **State Management**

Persistent state is tracked across dedicated files:

* **transfer\_checkpoint.json**: Tracks all successful transfers.

**Note:** Circuit breaker state (e.g., failure counts) is **ephemeral** and resets when the script exits.

## **Coding Standards**

| Standard | Requirement |
| :---- | :---- |
| **Python Version** | Python 3.10+ |
| **Type Hinting** | Use standard Python type hints consistently. |
| **Logging** | Use the SystemManager logger (or standard logging library). **DO NOT** use print() statements for operational logs. |

## **Common Tasks Guidance**

### **Refactoring**

* **Check Architecture:** Always check **ARCHITECTURE.md** to ensure you aren't creating circular dependencies or violating ownership rules.

### **New Features**

* **Placement:** Ensure any new logic is placed in the **appropriate Manager**.  
* **Avoid Bloat:** Do not bloat **TorrentMover** with low-level execution details.
