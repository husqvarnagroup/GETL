"""Common base class for file registry."""
from abc import ABC, abstractmethod


class FileRegistry(ABC):
    """Interface class for the file registry implmentations."""

    @abstractmethod
    def update(self):
        """Update the files that have been loaded."""
        pass
