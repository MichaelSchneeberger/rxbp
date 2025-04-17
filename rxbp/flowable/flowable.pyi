from abc import abstractmethod
from typing import Callable, Generator, override

from continuationmonad.typing import Scheduler

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.observeresult import ObserveResult
from rxbp.flowabletree.nodes import SingleChildFlowableNode
from rxbp.state import State

class Flowable[V](SingleChildFlowableNode[V, V]):
    # used for the donotation.do notation
    def __iter__(self) -> Generator[None, None, V]: ...
    @abstractmethod
    def copy(self, /, **changes) -> Flowable[V]: ...
    def flat_map[U](self, func: Callable[[V], Flowable[U]]) -> Flowable[U]: ...
    def share(self) -> Flowable[V]: ...
    def run(self, scheduler: Scheduler | None = None) -> list[V]: ...
    @override
    def unsafe_subscribe(
        self, state: State, observer: Observer[V]
    ) -> tuple[State, ObserveResult]: ...
