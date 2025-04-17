from abc import abstractmethod
from typing import Iterable, Iterator, override

from dataclassabc import dataclassabc

from donotation import do

import continuationmonad
from continuationmonad.typing import ContinuationMonad, ContinuationCertificate

from rxbp.cancellable import init_cancellation_state
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.observeresult import ObserveResult
from rxbp.state import State


class FromIterable[V](FlowableNode[V]):
    @property
    @abstractmethod
    def iterable(self) -> Iterable[V]: ...

    @override
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ) -> tuple[State, ObserveResult]:
        iterator = iter(self.iterable)

        @do()
        def schedule_and_send_next(current_item: V, iterator: Iterator[V]) -> ContinuationMonad:
            if state.scheduler:
                # schedule sending action on dedicated scheduler
                yield from continuationmonad.schedule_on(state.scheduler)

            try:
                next_item, has_further_items = next(iterator), True

            except StopIteration:
                # receive acknowledgment
                next_item, has_further_items = None, False

            if has_further_items:
                # receive acknowledgment
                _ = yield from args.observer.on_next(current_item)

                yield from continuationmonad.schedule_trampoline()

                return schedule_and_send_next(next_item, iterator)

            else:
                return args.observer.on_next_and_complete(current_item)
                # certificate = args.observer.on_next_and_complete(current_item)
                # return continuationmonad.from_(certificate)
                # match result := args.observer.on_next_and_complete(current_item):
                #     case ContinuationCertificate():
                #         return continuationmonad.from_(result)
                #     case ContinuationMonad():
                #         return result
                #     case _:
                #         raise Exception(f"Unexpected result {result}.")

        @do()
        def starting_procedure(iterator):
            try:
                next_item = next(iterator)

            except StopIteration:
                return args.observer.on_completed()
                # certificate = args.observer.on_completed()
                # return continuationmonad.from_(certificate)
                # match result := args.observer.on_completed():
                #     case ContinuationCertificate():
                #         return continuationmonad.from_(result)
                #     case ContinuationMonad():
                #         return result
                #     case _:
                #         raise Exception(f"Unexpected result {result}.")

            else:
                return schedule_and_send_next(next_item, iterator)

        cancellable = init_cancellation_state()

        # try:
        #     next_item = next(iterator)

        # except StopIteration:
        #     # continuation = args.observer.on_completed()
        #     certificate = args.observer.on_completed()

        # else:
        #     # continuation: ContinuationMonad = schedule_and_send_next(next_item, iterator)

        certificate = (
            continuationmonad.from_(None)
            .flat_map(lambda _: starting_procedure(iterator))
            .run_on_trampoline(
                trampoline=state.subscription_trampoline,  # ensures scheduling on trampoline
                cancellation=cancellable,
                weight=args.schedule_weight,
            )
        )

        result = ObserveResult(
            certificate=certificate,
            cancellable=cancellable,
        )

        return state, result


@dataclassabc(frozen=True)
class FromIterableImpl[V](FromIterable[V]):
    iterable: Iterable[V]


def init_from_iterable[V](iterable: Iterable[V]):
    return FromIterableImpl[V](iterable=iterable)
