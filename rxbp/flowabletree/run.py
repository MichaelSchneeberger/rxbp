
from dataclasses import dataclass, replace
from logging import Logger
import logging
from threading import Lock
from typing import Any, Self, override
from abc import abstractmethod, ABC


import contcert
from contcert.typing import (
    LoopLikeScheduler,
    Scheduler,
    ContCert,
)
import rxsingle

from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.notifications import Notification, OnCompleted, OnError, OnNext, OnNextAndComplete
from rxbp.flowabletree.operations.asyncshare.observer import AsyncShareObserver
from rxbp.flowabletree.operations.asyncshare.subject import AsyncShareSubscribeArgs, AsyncShareSubject
from rxbp.flowabletree.operations.share.flowable import ShareFlowableNode, ShareFlowableState
from rxbp.flowabletree.operations.share.observer import ShareObserver
from rxbp.flowabletree.subscribe import subscribe_single_sink
from rxbp.flowabletree.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.flowablenode import FlowableNode
# from rxbp.flowabletree.sources.connectable import ConnectableFlowableNode


@dataclass
class SubscribeArgsImpl(AsyncShareSubscribeArgs, SubscribeArgs):
    virtual_count: int
    async_index: int
    scheduler: Scheduler
    async_share_observers: dict[AsyncShareSubject[Any, Any, Any], AsyncShareObserver[Any, Any]]

    @property
    def p_virtual_count(self) -> int:
        return self.virtual_count

    @property
    def p_async_index(self) -> int: 
        return self.async_index

    # default scheduler when no explicit scheduler is provided
    @property
    def p_scheduler(self) -> Scheduler: 
        return self.scheduler

    def update_basics[V](
        self, /,
        virtual_count: int | None = None,
        async_index: int | None = None,
        scheduler: Scheduler | None = None,
    ) -> Self:
        def gen_args():
            if virtual_count is not None:
                yield 'virtual_count', virtual_count
            if async_index is not None:
                yield 'async_index', async_index
            if scheduler is not None:
                yield 'scheduler', scheduler
            # if async_share_observers is not None:
            #     yield 'async_share_observers', async_share_observers

        args = dict(gen_args())
        return replace(self, **args)

    @property
    def p_async_share_observers(
        self,
    ) -> dict[AsyncShareSubject[Any, Any, Any, Any], AsyncShareObserver[Any, Any]]:  # pyright: ignore[reportExplicitAny]
        return self.async_share_observers

    def update_async_share_observers(
        self,
        observers: dict[
            AsyncShareSubject[Any, Any, Any, Any], AsyncShareObserver[Any, Any]  # pyright: ignore[reportExplicitAny]
        ],
    ) -> Self:
        return replace(self, async_share_observers=observers)



# def init_subscribe_args(
#     virtual_count: int,
#     async_index: int,
#     scheduler: Scheduler,
#     async_share_observers: dict[AsyncShareSubject[Any, Any, Any], AsyncShareObserver[Any, Any]],  # pyright: ignore[reportExplicitAny]
# ) -> SubscribeArgs:
#     return SubscribeArgsImpl(
#         virtual_count=virtual_count,
#         async_index=async_index,
#         scheduler=scheduler,
#         async_share_observers=async_share_observers,
#     )



@dataclass
class StateImpl(ShareFlowableState, State):
# class StateImpl(AsyncShareFlowableState, ShareFlowableState, State):
    item_scheduler: LoopLikeScheduler
    default_scheduler: Scheduler
    lambda_trace_logger: Logger
    # rxbp_observer: Observer | None  # pyright: ignore[reportMissingTypeArgument]
    share_flowable_count: dict[ShareFlowableNode[ShareFlowableState, Any, Any], int]  # pyright: ignore[reportExplicitAny]
    share_virtual_count: dict[ShareFlowableNode[ShareFlowableState, Any, Any], int]  # pyright: ignore[reportExplicitAny]
    share_observers: dict[ShareFlowableNode[ShareFlowableState, Any, Any], ShareObserver[Any, Any]]  # pyright: ignore[reportExplicitAny]
    # async_share_observers_lock: Lock
    # async_share_observers: dict[AsyncShareKey[Self, Any, Any], AsyncShareObserver[Any, Any]]  # pyright: ignore[reportExplicitAny]

    @property
    @override
    def p_item_scheduler(self) -> LoopLikeScheduler:
        return self.item_scheduler

    @property
    @override
    def p_default_scheduler(self) -> Scheduler:
        return self.default_scheduler

    @property
    @override
    def p_lambda_trace_logger(self) -> Logger:
        return self.lambda_trace_logger

    @override
    def update_item_scheduler(self, scheduler: LoopLikeScheduler) -> Self:
        return replace(self, item_scheduler=scheduler)

    # @property
    # @override
    # def p_rxbp_observer(self) -> Observer | None:  # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
    #     return self.rxbp_observer  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
    #
    # @override
    # def update_rxbp_observer(self, observer: Observer | None) -> Self:  # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
    #     return replace(self, rxbp_observer=observer)

    @property
    @override
    def p_share_flowable_count(self) -> dict[ShareFlowableNode[ShareFlowableState, Any, Any], int]:  # pyright: ignore[reportExplicitAny]
        return self.share_flowable_count

    @property
    @override
    def p_share_virtual_count(self) -> dict[ShareFlowableNode[ShareFlowableState, Any, Any], int]:   # pyright: ignore[reportExplicitAny]
        return self.share_virtual_count

    @property
    @override
    def p_share_observers(
        self,
    ) -> dict[ShareFlowableNode[ShareFlowableState, Any, Any], ShareObserver[Any, Any]]:   # pyright: ignore[reportExplicitAny]
        return self.share_observers

    # @property
    # @override
    # def p_async_share_observers_lock(self) -> Lock:
    #     return self.async_share_observers_lock
    #
    # @property
    # @override
    # def p_async_share_observers(self) -> dict[AsyncShareKey[Self, Any, Any], AsyncShareObserver[Any, Any]]:   # pyright: ignore[reportExplicitAny]
    #     return self.async_share_observers

    @override
    def update_shared_flowable_data[U, E](
        self,
        flowable_count: dict[ShareFlowableNode[ShareFlowableState, U, E], int] | None = None,
        virtual_count: dict[ShareFlowableNode[ShareFlowableState, U, E], int] | None = None,
        share_observers: tuple[ShareFlowableNode[ShareFlowableState, U, E], ShareObserver[U, E]] | None = None,
    ) -> ShareFlowableState:
        def gen_args():
            if flowable_count is not None:
                yield 'share_flowable_count', flowable_count
            if virtual_count is not None:
                yield 'share_virtual_count', virtual_count
            if share_observers is not None:
                n_share_observers = self.share_observers | {share_observers[0]: share_observers[1]}
                yield 'share_observers', n_share_observers

        return replace(self, **dict(gen_args()))


def run[U, E](
    source: FlowableNode[State, U, E],
    async_index: int | None = None,
    lambda_trace_logger: Logger | None = None,
    # scheduler: MainScheduler | None = None,
    # connections: dict[ConnectableFlowableNode[S, U], FlowableNode[S, U]] | None = None,
):
    if async_index is None:
        async_index = 1

    if lambda_trace_logger is None:
        lambda_trace_logger = logging.getLogger("LambdaTrace")
        lambda_trace_logger.setLevel(logging.DEBUG)

    scheduler = contcert.init_contained_event_loop_scheduler(
        lambda_trace_logger=lambda_trace_logger,
    )
    trampoline = contcert.init_trampoline_scheduler(

        lambda_trace_logger=lambda_trace_logger,
    )
    # if scheduler is None:
    #     scheduler = contcert.init_main_scheduler()
    #
    # subscribe_trampoline = contcert.init_trampoline()
    virtual_count = 1

    @dataclass()
    class MainObserver(Observer[U, E]):
        received_items: list[U]
        received_exception: Exception | None
        is_completed: bool

        @override
        def notify(
            self,
            notification: Notification[U, E],
        ) -> ContCert:
            match notification:
                case OnNext():
                    self.received_items.append(notification.p_item)
                    return notification.p_observer.notify(
                        rxsingle.init_on_success(
                            item=None,
                            trampoline=notification.p_trampoline,
                            item_scheduler=notification.p_item_scheduler,
                        )
                    )

                case OnNextAndComplete():
                    self.received_items.append(notification.p_item)
                    self.is_completed = True
                    scheduler.stop()
                    cert = scheduler.acquire_certificate(
                        virtual_count=virtual_count,
                        stacks=tuple(),
                    )
                    return cert

                case OnCompleted():
                    # print('on_completed()')
                    self.is_completed = True
                    scheduler.stop()
                    return scheduler.acquire_certificate(
                        virtual_count=virtual_count,
                        stacks=tuple(),
                    )

                case OnError():
                    self.received_exception = notification.p_exception
                    self.is_completed = True
                    scheduler.stop()
                    return scheduler.acquire_certificate(
                        virtual_count=virtual_count,
                        stacks=tuple(),
                    )

    # if connections is None:
    #     connections = {}

    observer = MainObserver(
        received_items=[],
        received_exception=None,
        is_completed=False,
    )

    # def schedule_task():
    #     def trampoline_task():
    #         state = init_state(
    #             subscription_trampoline=subscribe_trampoline,
    #             # scheduler=scheduler,
    #         )
    #
    #         return subscribe_single_sink(
    #             source=source,
    #             args=init_subscribe_args(
    #                 observer=observer,
    #                 virtual_count=1,
    #                 scheduler=scheduler,
    #             ),
    #             state=state,
    #             connections=connections,
    #         )
    #
    #     # return result.certificate
    #
    #     return subscribe_trampoline.run(trampoline_task, virtual_count=1)
    #
    # scheduler.run(schedule_task, virtual_count=1, cancellation=None)


    state = StateImpl(
        # min_subscription_async_index=0,
        lambda_trace_logger=lambda_trace_logger,
        default_scheduler=scheduler,
        item_scheduler=scheduler,
        # rxbp_observer=None,
        share_flowable_count={},
        share_virtual_count={},
        share_observers={},
        # async_share_observers_lock=Lock(),
        # async_share_observers={},
    )

    # state = source.discover(state)

    # async_index = state.min_subscription_context

    def scheduler_callable():

        def trampoline_callable() -> ContCert:
            args = SubscribeArgsImpl(
                scheduler=scheduler,
                virtual_count=virtual_count,
                async_index=async_index,
                async_share_observers={},
            )

            _, result = subscribe_single_sink(
                source=source,
                state=state,
                observer=observer,
                args=args,
            )

            return result.certificate

        task = contcert.init_task(
            callable=trampoline_callable,
            virtual_count=virtual_count,
            async_index=async_index,
            cancellation=None,
        )
        return trampoline.run(task=task, stacks=tuple())

    task = contcert.init_task(
        callable=scheduler_callable,
        virtual_count=virtual_count,
        async_index=async_index,
        cancellation=None,
    )

    _ = scheduler.run(
        task=task,
    )

    assert observer.is_completed

    if observer.received_exception:
        raise observer.received_exception

    return observer.received_items
