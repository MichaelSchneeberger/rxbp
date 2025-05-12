from dataclasses import dataclass
from threading import Lock
import traceback
from typing import Callable

from donotation import do

import continuationmonad
from continuationmonad.abc import ContinuationMonadOperatorException
from continuationmonad.typing import (
    DeferredHandler,
    Scheduler,
)

from rxbp.exceptions import RxBpException
from rxbp.state import init_state
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeandconnect import subscribe_single_sink
from rxbp.flowabletree.operations.flatmap.innerobserver import FlatMapInnerObserver
from rxbp.flowabletree.operations.flatmap.sharedmemory import FlatMapSharedMemory
from rxbp.flowabletree.operations.flatmap.states import (
    ActiveStateMixin,
    HasTerminatedState,
    OnCompletedState,
    OnErrorState,
    StopContinuationStateMixin,
    TerminatedStateMixin,
)
from rxbp.flowabletree.operations.flatmap.statetransitions import (
    OnCompletedOuterTransition,
    OnErrorOuterTransition,
    OnNextAndCompleteOuterTransition,
    OnNextOuterTransition,
)
from rxbp.utils.framesummary import FrameSummary, to_execution_exception_message, to_operator_exception_message


@dataclass
class FlatMapObserver[U, V](Observer):
    shared: FlatMapSharedMemory
    lock: Lock
    last_id: int
    weight: int
    scheduler: Scheduler
    func: Callable[[U], FlowableNode[V]]
    stack: tuple[FrameSummary, ...]
    raise_immediately: bool

    @do()
    def _on_next(self, item: U, handler: DeferredHandler | None):
        try:
            flowable = self.func(item)

        except ContinuationMonadOperatorException as exception:
            if self.raise_immediately:
                raise

            exception = RxBpException(
                "\n".join(
                    (
                        exception.args[0],
                        to_execution_exception_message(traceback.format_exc()),
                    )
                )
            )
            return self.shared.downstream.on_error(exception)

        except Exception:
            if self.raise_immediately:
                raise RxBpException(
                    to_operator_exception_message(stack=self.stack)
                )
    
            exception = RxBpException(
                "\n".join(
                    (
                        to_execution_exception_message(traceback.format_exc()),
                        to_operator_exception_message(stack=self.stack),
                    )
                )
            )
            return self.shared.downstream.on_error(exception)
        
        trampoline = yield from continuationmonad.get_trampoline()

        with self.lock:
            id = self.last_id
            self.last_id += 1

        state = init_state(
            subscription_trampoline=trampoline,
            scheduler=self.scheduler,
        )

        sink = FlatMapInnerObserver(
            id=id,
            shared=self.shared,
        )

        try:
            state, result = subscribe_single_sink(
                source=flowable,
                sink=sink,
                state=state,
                weight=self.weight,
            )

        except ContinuationMonadOperatorException as exception:
            if self.raise_immediately:
                raise

            exception = RxBpException(
                "\n".join(
                    (
                        exception.args[0],
                        to_execution_exception_message(traceback.format_exc()),
                    )
                )
            )
            return self.shared.downstream.on_error(exception)

        except Exception:
            if self.raise_immediately:
                raise RxBpException(
                    to_operator_exception_message(stack=self.stack)
                )
    
            exception = RxBpException(
                "\n".join(
                    (
                        to_execution_exception_message(traceback.format_exc()),
                        to_operator_exception_message(stack=self.stack),
                    )
                )
            )
            return self.shared.downstream.on_error(exception)

        # state, result = flowable.unsafe_subscribe(
        #     state=init_state(
        #         subscription_trampoline=trampoline,
        #         scheduler=self.scheduler,
        #     ),
        #     args=SubscribeArgs(
        #         observer=FlatMapInnerObserver(
        #             id=id,
        #             shared=self.shared,
        #         ),
        #         schedule_weight=self.schedule_weight,
        #     ),
        # )

        if handler is None:
            transition = OnNextAndCompleteOuterTransition(
                child=None,  # type: ignore
                certificate=result.certificate,
            )
        else:
            transition = OnNextOuterTransition(
                child=None,  # type: ignore
                certificate=result.certificate,
            )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case StopContinuationStateMixin(certificate=certificate):
                return continuationmonad.from_(certificate)

            case ActiveStateMixin():
                if handler is None:
                    raise Exception(f"Unexpected state {state}.")

                else:
                    certificate = handler.resume(trampoline, None)
                    return continuationmonad.from_(certificate)

            case TerminatedStateMixin(outer_certificate=outer_certificate):
                return continuationmonad.from_(outer_certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_next(self, item: U):
        # print(f"on_next({item})")

        def on_next_subscription(_, handler: DeferredHandler):
            return self._on_next(item, handler)

        return continuationmonad.defer(on_next_subscription)

    def on_next_and_complete(self, item: U):
        # print(f"on_next_and_completed({item})")
        return self._on_next(item, None)

    def on_completed(self):
        transition = OnCompletedOuterTransition(
            child=None,  # type: ignore
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnCompletedState():
                return self.shared.downstream.on_completed()

            case StopContinuationStateMixin(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")

    def on_error(self, exception: Exception):
        transition = OnErrorOuterTransition(
            child=None,  # type: ignore
            exception=exception,
        )

        with self.shared.lock:
            transition.child = self.shared.transition
            self.shared.transition = transition

        match state := transition.get_state():
            case OnErrorState(
                exception=exception,
                certificates=certificates,
            ):
                for id, certificate in certificates.items():
                    self.shared.cancellables[id].cancel(certificate)

                return self.shared.downstream.on_error(exception)

            case HasTerminatedState(certificate=certificate):
                return continuationmonad.from_(certificate)

            case _:
                raise Exception(f"Unexpected state {state}.")
