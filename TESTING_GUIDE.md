# Testing Guide

This document outlines the testing standards and practices for the Torrent Mover project. It is designed to ensure high code quality and prevent regressions.

## 1. Test Organization

The test suite is organized to mirror the source code structure.

*   **Source Directory:** `./` (Root)
*   **Test Directory:** `tests/`
*   **Mocks Directory:** `tests/mocks/`

For every source file `example_manager.py`, there should be a corresponding test file `tests/test_example_manager.py`.

Mocks should be placed in `tests/mocks/` to be shared across multiple test files if necessary, or defined within the test file if specific to that test case.

## 2. How to Add a Test

Follow these steps when adding a new test case:

1.  **Fixture Setup:**
    *   Initialize the necessary objects and mocks.
    *   Ensure isolation from the real system (e.g., mock filesystem operations, network calls).

2.  **Happy Path:**
    *   Write a test case that verifies the expected behavior under normal conditions.
    *   Assert the correct output or state change.

3.  **Edge Cases:**
    *   Write test cases for boundary conditions (e.g., empty inputs, large values).
    *   Test for error handling (e.g., exceptions, network failures).

4.  **Cleanup:**
    *   Ensure any resources created during the test are cleaned up (teardown).
    *   Reset any global state or mocks.

## 3. Critical Areas Needing Tests

The following areas are critical and require thorough testing:

*   **Race Conditions in UIManager Updates:**
    *   Verify that concurrent updates to the UI do not cause crashes or inconsistencies.
    *   Ensure thread safety mechanisms (locks) are working as expected.

*   **FileTransferTracker Corruption Expiry:**
    *   Test the logic that handles corrupted files and their expiration from the tracker.
    *   Ensure that the corruption flag is correctly set and cleared.

*   **Progress Clamping Logic:**
    *   Verify that progress values are clamped between 0% and 100%.
    *   Ensure that delta calculation errors do not result in progress > 100%.

*   **Category Precedence (Mandatory):**
    *   Verify that Tracker Rules take precedence over the Source Category.
    *   Test scenarios where both are present and ensure the correct category is assigned.

*   **Eligibility (Mandatory):**
    *   Verify that only torrents with 100% completion are eligible for transfer.
    *   Test with incomplete torrents to ensure they are filtered out.
