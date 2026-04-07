from abc import ABC, abstractmethod


class AssignVirtualCount[S](ABC):
    @abstractmethod
    def discover(self, state: S) -> S: ...

    @abstractmethod
    def assign_virtual_count(self, state: S, virtual_count: int) -> S: ...
