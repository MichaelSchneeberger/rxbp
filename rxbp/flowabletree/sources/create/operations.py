from abc import ABC
from dataclasses import dataclass, field
from typing import Never, override

from lambdatrace import StackTrace

from contcert.typing import ContCert, LoopLikeScheduler, Trampoline

from rxbp.flowabletree.notifications import init_on_completed, init_on_next, init_on_next_and_complete
import rxsingle
from rxsingle.abc import (
    Observer as RxSingleObserver,
    SubscribeArgs as RxSingleSubscribeArgs,
    RxSingleLeave,
    RxSingle,
)

from rxbp.flowabletree.state import State
from rxbp.flowabletree.observer import Observer


@dataclass
class CreateSubscribeArgs[U, E](RxSingleSubscribeArgs, ABC):
    rxbp_observer: Observer[U, E]


@dataclass(frozen=True)
class OnNextRxSingleLeave[S: State, U, E](RxSingleLeave[S, CreateSubscribeArgs[U, E], None, Never]):
    item: U
    stacks: tuple[StackTrace, ...] = field(repr=False)

    @override
    def _subscribe(
        self,
        state: S,
        observer: RxSingleObserver[None, Never],
        args: CreateSubscribeArgs[U, E],
        trampoline: Trampoline,
        item_scheduler: LoopLikeScheduler,
    ) -> ContCert:

        # if state.p_rxbp_observer is None:  # pyright: ignore[reportUnknownMemberType]
        #     raise Exception("not good")
        #     # Todo: Complete this
        # args.observer.on_error(Exception('not good'))
        #
        # # args_: CreateSubscribeArgs[U] = args
        # args_ = cast(CreateSubscribeArgs[U], args)

        cert = args.rxbp_observer.notify(
            init_on_next(
                item=self.item,
                trampoline=trampoline,
                item_scheduler=item_scheduler,
                observer=observer,
            )
        )
        return cert

    @property
    @override
    def p_stacks(self) -> tuple[StackTrace, ...]:
        return self.stacks


def init_on_next_rx_single[S: State, U, E](item: U) -> RxSingle[S, CreateSubscribeArgs[U, E], None, Never]:
    return rxsingle.init_rx_single(
        child=OnNextRxSingleLeave[S, U, E](item=item, stacks=tuple())
    )


@dataclass(frozen=True)
class OnNextAndCompletedRxSingleLeave[S: State, U, E](RxSingleLeave[S, CreateSubscribeArgs[U, E], ContCert, Never]):
    item: U
    stacks: tuple[StackTrace, ...] = field(repr=False)

    @override
    def _subscribe(
        self,
        state: S,
        observer: RxSingleObserver[ContCert, Never],
        args: CreateSubscribeArgs[U, E],
        trampoline: Trampoline,
        item_scheduler: LoopLikeScheduler,
    ) -> ContCert:

        # if state.p_rxbp_observer is None:  # pyright: ignore[reportUnknownMemberType]
        #     raise Exception("not good")

        cert = args.rxbp_observer.notify(
            init_on_next_and_complete(
                item=self.item,
                trampoline=trampoline,
                item_scheduler=item_scheduler,
            )
        )
        return observer.notify(
            rxsingle.init_on_success(
                item=cert,
                trampoline=trampoline,
                item_scheduler=item_scheduler,
            ),
        )

    @property
    @override
    def p_stacks(self) -> tuple[StackTrace, ...]:
        return self.stacks


class init_on_next_and_complete_rx_single[S: State, U, E]:
    def __new__(cls, item: U) -> RxSingle[S, CreateSubscribeArgs[U, E], ContCert, Never]:
        return rxsingle.init_rx_single(
            child=OnNextAndCompletedRxSingleLeave[S, U, E](item=item, stacks=tuple())
        )


@dataclass(frozen=True)
class OnCompletedRxSingleLeave[S: State, U, E](RxSingleLeave[S, CreateSubscribeArgs[U, E], ContCert, E]):
    stacks: tuple[StackTrace, ...] = field(repr=False)

    @override
    def _subscribe(
        self,
        state: S,
        observer: RxSingleObserver[ContCert, Never],
        args: CreateSubscribeArgs[U, E],
        trampoline: Trampoline,
        item_scheduler: LoopLikeScheduler,
    ) -> ContCert:

        # if state.p_rxbp_observer is None:  # pyright: ignore[reportUnknownMemberType]
        #     raise Exception("not good")
        # if not isinstance(args, CreateSubscribeArgs):
        #     raise Exception(f'The args {args} is not of type CreateSubscribeArgs.')
        #
        # args_ = cast(CreateSubscribeArgs[ContCert], args)

        cert = args.rxbp_observer.notify(
            init_on_completed(
                trampoline=trampoline,
                item_scheduler=item_scheduler,
            )
        )

        return observer.notify(
            rxsingle.init_on_success(
                item=cert,
                trampoline=trampoline,
                item_scheduler=item_scheduler,
            ),
        )

    @property
    @override
    def p_stacks(self) -> tuple[StackTrace, ...]:
        return self.stacks


class init_on_completed_rx_single[S: State, U, E]:
    def __new__(cls) -> RxSingle[S, CreateSubscribeArgs[U, E], ContCert, E]:
        return rxsingle.init_rx_single(
            child=OnCompletedRxSingleLeave[S, U, E](stacks=tuple())
        )


