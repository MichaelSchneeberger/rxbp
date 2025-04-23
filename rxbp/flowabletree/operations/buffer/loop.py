from __future__ import annotations

from dataclasses import dataclass
from threading import RLock

from donotation import do

import continuationmonad
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.buffer.states import (
    CompleteState,
    LoopActive,
    SendErrorState,
    SendItemAndComplete,
    StopLoop,
)
from rxbp.flowabletree.operations.buffer.transitions import (
    RequestTransition,
    ShareTransition,
)

@dataclass
class Loop[V]:
    observer: Observer
    transition: ShareTransition
    lock: RLock
    buffer: list[V]

    @do()
    def run(self):
        transition = RequestTransition(child=None)

        with self.lock:
            transition.child = self.transition
            self.transition = transition

        match transition.get_state():
            case LoopActive():
                item = self.buffer.pop(0)
                _ = yield from self.observer.on_next(item)
                
                return self.run()

            case SendItemAndComplete():
                item = self.buffer.pop()
                return self.observer.on_next_and_complete(item)
            
            case StopLoop(certificate=certificate):
                return continuationmonad.from_(certificate)
            
            case CompleteState():
                return self.observer.on_completed()

            case SendErrorState(exception=exception):
                return self.observer.on_error(exception)