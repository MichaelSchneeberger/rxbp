from dataclasses import dataclass
from typing import Callable, Generic
from typing import TypeVar

OutputType = TypeVar('OutputType')


@dataclass
class PipeOperation(Generic[OutputType]):
    func: Callable[[OutputType], OutputType]

    def __call__(self, source: OutputType):
        return self.func(source)
