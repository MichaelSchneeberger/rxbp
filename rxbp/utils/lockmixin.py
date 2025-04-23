from abc import ABC, abstractmethod
from threading import RLock


class LockMixin(ABC):
    @property
    @abstractmethod
    def lock(self) -> RLock: ...
