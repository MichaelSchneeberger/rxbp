from abc import abstractmethod
from typing import Generator, override

from continuationmonad.typing import Scheduler

from rxbp.flowabletree.data.observer import Observer
from rxbp.flowabletree.data.observeresult import ObserveResult
from rxbp.flowabletree.nodes import SingleChildFlowableNode
from rxbp.state import State

class Flowable[V](SingleChildFlowableNode[V, V]):
    # used for the donotation.do notation
    def __iter__(self) -> Generator[None, None, V]: ...
    @abstractmethod
    def copy(self, /, **changes) -> Flowable[V]: ...
    def share(self) -> Flowable[V]: ...
    def run(self, scheduler: Scheduler | None = None) -> list[V]: ...
    @override
    def unsafe_subscribe(
        self, state: State, observer: Observer[V]
    ) -> tuple[State, ObserveResult]: ...
