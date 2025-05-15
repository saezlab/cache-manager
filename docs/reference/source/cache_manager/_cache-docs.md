## Description

The `_cache.py` file is the core implementation of the cache management system for the `cache_manager` package. It defines the `Cache` class, which provides a high-level interface for storing, retrieving, and managing cached data and associated metadata using a SQLite database and a filesystem directory.

### Main Components

- **Cache Class:**
  The main interface for users, offering methods such as `create`, `search`, `update`, `remove`, `move_in`, and more.

- **Helper Methods:**
  Internal methods for SQL query construction, schema management, and attribute handling.

- **Integration with Other Modules:**
  Relies on supporting modules (e.g., `_item`, `_session`, `_status` ,`utils`) for item representation, logging, status management, and utility functions.

---

::: cache_manager._cache
