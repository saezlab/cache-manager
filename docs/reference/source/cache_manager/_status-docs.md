## Description

The `_status.py` file defines the `Status` enumeration used throughout the `cache_manager` package to represent the state and lifecycle of cache items. Each status value corresponds to a specific stage in a cache item's existence, such as uninitialized, being written, ready, failed, trashed, or deleted. This module provides a clear, type-safe, and human-readable way to manage and check the status of cache entries during all cache operations.

### Main Components

- **Status Enum Class:**
  Enumerates all possible states for a cache item, including:

  | State               | Description                                                           |
  | ------------------- | --------------------------------------------------------------------- |
  | `UNINITIALIZED` (0) | Newly created entry, not yet initialized.                             |
  | `WRITE` (1)         | Item is currently being written.                                      |
  | `FAILED` (2)        | An error occurred during processing.                                  |
  | `READY` (3)         | Entry is ready and available.                                         |
  | `TRASH` (-1)        | Entry is marked for deletion but can be recovered; file still exists. |
  | `DELETED` (-2)      | File is deleted and entry is marked for permanent removal.            |


- **Enum Integration:**
  Inherits from Python's `enum.Enum` for robust, type-safe status management.

- **Usage Examples:**
  Demonstrates how to instantiate and use the `Status` enum for cache item state tracking.

---

::: cache_manager._status
