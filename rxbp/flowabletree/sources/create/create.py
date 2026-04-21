from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, cast, override


from lambdatrace import StackTrace

import contcert
from contcert.typing import ContCert, Scheduler, Trampoline

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
from rxbp.flowabletree.subscriptionresult import SubscriptionResult, init_subscription_result
from rxbp.flowabletree.flowablenode import FlowableNode


# @dataclass
# class CreateSubscribeArgs[U](RxSingleSubscribeArgs[ContCert]):
#     rxbp_observer: Observer[U]


@dataclass(frozen=True)
class OnNextRxSingleLeave[S: State, U](RxSingleLeave[S, None]):
    item: U

    @override
    def _subscribe(
        self,
        state: S, 
        args: RxSingleSubscribeArgs[None],
    ) -> ContCert:

        if state.p_rxbp_observer is None:  # pyright: ignore[reportUnknownMemberType]
            raise Exception('not good')
        #     # Todo: Complete this
            # args.observer.on_error(Exception('not good'))
        #
        # # args_: CreateSubscribeArgs[U] = args  # pyright: ignore[reportUnknownVariableType]
        # args_ = cast(CreateSubscribeArgs[U], args)

        cert = state.p_rxbp_observer.on_next(  # pyright: ignore[reportUnknownMemberType]
            trampoline=state.p_subscription_trampoline,
            item=self.item,
            observer=args.observer,
        )
        return cert


def init_on_next[S: State, U](item: U) -> RxSingle[S, None]:  # pyright: ignore[reportInvalidTypeVarUse]
    return rxsingle.init_rx_single(child=OnNextRxSingleLeave[S, U](item=item, stacks=tuple()))


@dataclass(frozen=True)
class OnNextAndCompletedRxSingleLeave[S: State, U](RxSingleLeave[S, ContCert]):
    item: U

    @override
    def _subscribe(
        self,
        state: S, 
        args: RxSingleSubscribeArgs[ContCert],
    ) -> ContCert:

        if state.p_rxbp_observer is None:  # pyright: ignore[reportUnknownMemberType]
            raise Exception('not good')

        cert = state.p_rxbp_observer.on_next_and_complete(state.p_subscription_trampoline, self.item)  # pyright: ignore[reportUnknownMemberType]
        return args.observer.on_success(state.p_subscription_trampoline, cert)


class init_on_next_and_complete[S: State, U]:
    def __new__(cls, item: U) -> RxSingle[S, ContCert]:
        return rxsingle.init_rx_single(child=OnNextAndCompletedRxSingleLeave[S, U](item=item, stacks=tuple()))

@dataclass(frozen=True)
class OnCompletedRxSingleLeave[S: State](RxSingleLeave[S, ContCert]):
    @override
    def _subscribe(
        self,
        state: S, 
        args: RxSingleSubscribeArgs[ContCert],
    ) -> ContCert:

        if state.p_rxbp_observer is None:  # pyright: ignore[reportUnknownMemberType]
            raise Exception('not good')
        # if not isinstance(args, CreateSubscribeArgs):
        #     raise Exception(f'The args {args} is not of type CreateSubscribeArgs.')
        #
        # args_ = cast(CreateSubscribeArgs[ContCert], args)

        cert = state.p_rxbp_observer.on_completed()  # pyright: ignore[reportUnknownMemberType]
        return args.observer.on_success(state.p_subscription_trampoline, cert)


class init_on_completed[S: State]:
    def __new__(cls) -> RxSingle[S, ContCert]:
        return rxsingle.init_rx_single(child=OnCompletedRxSingleLeave[S](stacks=tuple()))


@dataclass
class StopObserver[U](RxSingleObserver[ContCert]):
    observer: Observer[U]

    @override
    def on_success(self, trampoline: Trampoline, item: ContCert):
        return item

    @override
    def on_error(
        self, trampoline: Trampoline, exception: Exception
    ) -> ContCert:
        return self.observer.on_error(exception)


@dataclass(frozen=True)
class Create[S: State, A, U](FlowableNode[S, U]):
    source: RxSingleNode[S, ContCert]
    stacks: tuple[StackTrace, ...]

    @override
    def unsafe_subscribe(
        self,
        state: S,
        args: SubscribeArgs[U],
    ) -> tuple[S, SubscriptionResult]:
        cancellable = init_cancellation_state()

        state, cert = self.source.subscribe(
            state=state.update_rxbp_observer(args.observer),  # pyright: ignore[reportUnknownMemberType]
            args=rxsingle.init_subscribe_args(
                observer=StopObserver(
                    observer=args.observer,
                ),
                virtual_count=args.virtual_count,
                context=args.context,
                cancellation=cancellable,
                # rxbp_observer=args.observer,
            )
        )

        state = state.update_rxbp_observer(None)  # pyright: ignore[reportUnknownMemberType]

        result = init_subscription_result(
            cancellable=cancellable,
            certificate=cert,
        )

        return state, result


def init_create[S: State, A, U](
    source: RxSingleNode[S, ContCert]
):
    return Create[S, A, U](
        source=source,
        stacks=tuple(),
    )
