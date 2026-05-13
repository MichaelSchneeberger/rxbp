from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Never, override


from lambdatrace import StackTrace

from contcert.typing import ContCert, Trampoline

from rxbp.flowabletree.notifications import init_on_error
from rxbp.flowabletree.sources.create.operations import CreateSubscribeArgs
import rxsingle
from rxsingle.abc import (
    Observer as RxSingleObserver,
    SubscribeArgs as RxSingleSubscribeArgs,
    RxSingleNode,
    RxSingleLeave,
    RxSingle,
)

from rxbp.cancellable import init_cancellation_state
from rxbp.flowabletree.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import (
    SubscriptionResult,
    init_subscription_result,
)
from rxbp.flowabletree.flowablenode import FlowableNode
from typing import Self
from rxsingle.abc import State as RxSingleState
from rxsingle.typing import Notification, OnError, OnSuccess


@dataclass
class StopObserver[U, E](RxSingleObserver[ContCert, Never]):
    observer: Observer[U, E]

    @override
    def notify(self, notification: Notification[ContCert, Never]) -> ContCert:
        match notification:
            case OnSuccess():
                return notification.p_item

            case OnError():
                return self.observer.notify(
                    init_on_error(
                        exception=notification.p_exception,
                        trampoline=notification.p_trampoline,
                        item_scheduler=notification.p_item_scheduler,
                    )
                )


@dataclass(frozen=True)
class Create[S: State, U, E](FlowableNode[S, U, E]):
    source: RxSingleNode[S, CreateSubscribeArgs[U, E], ContCert, Never]
    stacks: tuple[StackTrace, ...]

    @override
    def unsafe_subscribe(
        self,
        state: S,
        observer: Observer[U, E],
        args: SubscribeArgs,
    ) -> tuple[S, SubscriptionResult]:
        cancellable = init_cancellation_state()

        state, cert = self.source.subscribe(
            state=state,
            observer=StopObserver(
                observer=observer,
            ),
            args=CreateSubscribeArgs(
                virtual_count=args.virtual_count,
                async_index=args.async_index,
                cancellation=cancellable,
                rxbp_observer=observer,
            ),
        )

        # state = state.update_rxbp_observer(None)  # pyright: ignore[reportUnknownMemberType]

        result = init_subscription_result(
            cancellable=cancellable,
            certificate=cert,
        )

        return state, result


def init_create[S: State, U, E](source: RxSingleNode[S, CreateSubscribeArgs[U, E], ContCert, Never]):
    return Create[S, U, E](
        source=source,
        stacks=tuple(),
    )
