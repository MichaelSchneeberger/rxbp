from abc import ABC, abstractmethod


class AssignVirtualCount[State](ABC):
    @abstractmethod
    def discover(self, state: State) -> State: ...

    @abstractmethod
    def assign_virtual_count(self, state: State, virtual_count: int) -> State: ...
