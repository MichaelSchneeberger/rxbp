

from abc import abstractmethod, ABC
from dataclasses import dataclass

from dataclassabc import dataclassabc


class ObserverState(ABC): ...


class OnCompletedState(ObserverState): ...



@dataclass
class ErrorState(ObserverState):
    exception: Exception
    certificates: tuple[ContinuationCertificate, ...]
    # @property
    # @abstractmethod
    # def exception(self) -> Exception: ...

@dataclass
class TerminatedState(ObserverState): ...
    # certificates


class ObserverAction(ABC):
    @abstractmethod
    def get_state(self) -> ObserverState: ...


class ActionWithChild(ObserverAction):
    @property
    @abstractmethod
    def child(self) -> ObserverAction: ...


class HandleTerminationAction(ActionWithChild):
    @abstractmethod
    def _get_state(self, prev_state: ObserverState) -> ObserverState | None: ...

    def get_state(self) -> ObserverState:
        prev_state = self.child.get_state()

        match prev_state:
            case OnCompletedState() | ErrorState() | TerminatedState():
                return TerminatedState()
            
            case _:
                state = self._get_state(prev_state)
                if state is None:
                    raise Exception(f'{self}._get_state({prev_state}) returned None.')
                return state
            

class OnCompletedAction(HandleTerminationAction):
    def _get_state(self, _: ObserverState) -> ObserverState:
        return OnCompletedState()

@dataclassabc(frozen=True)
class OnErrorAction(HandleTerminationAction):
    child: ObserverAction
    exception: Exception
    # @property
    # @abstractmethod
    # def exception(self) -> Exception: ...

    def _get_state(self, _: ObserverState) -> ObserverState:
        return ErrorState(exception=self.exception)
