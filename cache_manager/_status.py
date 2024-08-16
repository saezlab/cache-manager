from __future__ import annotations

__all__ = [
    'Status',
]

import enum


class Status(enum.Enum):
    """
    Class defining the status for cache items.

    Arg:
        Integer defining current status as described above.

    Returns:
        Instance of `Status` with the specified status value provided.

    Attrs:
        UNINITIALIZED:
            Has value 0. Newly created entry, to be initialized.
        WRITE:
            Has value 1. Information currently being written on the item.
        FAILED:
            Has value 2. Something went wrong.
        READY:
            Has value 3. Entry is ready/initialized and not under writing.
        TRASH:
            Has value -1. Entry is to be deleted but can be recovered, file
            still exists.
        DELETED:
            Has value -2. File is deleted and entry is marked for deletion.

    Example:
        >>> Status(1)
        <Status.WRITE: 1>
        >>> Status.WRITE
        <Status.WRITE: 1>
    """

    UNINITIALIZED = 0
    WRITE = 1
    FAILED = 2
    READY = 3
    TRASH = -1
    DELETED = -2


    @classmethod
    def from_str(cls, name: str) -> Status:
        """
        Creates an instance of `Status` based on the passed status string
        instead of the integer.

        Arg:
            name:
                String defining the status. Not case-sensitive.

        Returns:
            Instance of `Status` with the specified status value provided.

        Example:
            >>> Status('WRITE')
            <Status.WRITE: 1>
            >>> Status('ready')
            <Status.READY: 3>
        """

        return cls.__dict__[name.upper()]
