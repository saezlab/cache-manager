from __future__ import annotations

import enum

__all__ = ['status']


class status(enum.Enum): # FIXME: CamelCase for class?
    """
    Class defining status for cache items. Satus values define the following:
    - UNINITIALIZED: 0 - Newly created entry, to be initialized
    - WRITE: 1 - Information currently being written on the item
    - FAILED: 2 - Something went wrong
    - READY: 3 - Entry is ready/initialized and not under writing
    - TRASH: -1 - Entry is to be deleted but can be recovered, file still exists
    - DELETED: -2 - File is deleted and entry is marked for deletion

    Arg:
        Integer defining current status as described above.

    Returns:
        Instance of status with the specified status value provided.

    Example:
        >>> status(1)
        <status.WRITE: 1>
    """

    UNINITIALIZED = 0
    WRITE = 1
    FAILED = 2
    READY = 3
    TRASH = -1
    DELETED = -2


    @classmethod
    def from_str(cls, name: str) -> status:
        """
        Creates an instance of status based on the passed status string instead
        of the integer.

        Arg:
            name:
                String defining the status

        Returns:
            Instance of status with the specified status value provided.

        Example:
            >>> status('WRITE')
            <status.WRITE: 1>
        """

        return cls.__dict__[name.upper()]
