from abc import ABC, abstractmethod


class ExecutionModelMixin(ABC):
    @abstractmethod
    def next_frame_index(self, current: int) -> int:
        ...