from dataclasses import dataclass, replace
from logging import Logger
import logging
from threading import Lock, RLock
from typing import Any, Callable, Self, override

# import reactivex
# import reactivex.disposable
# from reactivex import (
#     Observer as RxObserver,
#     Observable as RxObservable,
# )

import contcert
from contcert.typing import (
    Scheduler,
    ContCert,
    Cancellation,
    Trampoline,
)

from rxbp.flowabletree.operations.asyncshare.observer import AsyncShareObserver
from rxbp.flowabletree.operations.asyncshare.subject import AsyncShareFlowableState, AsyncShareKey
from rxbp.flowabletree.operations.share.flowable import ShareFlowableNode, ShareFlowableState
from rxbp.flowabletree.operations.share.observer import ShareObserver
from rxbp.flowabletree.subscribe import subscribe_single_sink
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
import rxsingle
from rxsingle.typing import (

    SubscribeArgs as RxSingleSubscribeArgs,
    Observer as RxSingleObserver,
)

from rxbp.cancellable import CancellationState
from rxbp.flowabletree.state import State
from rxbp.flowabletree.subscribeargs import init_subscribe_args
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.flowablenode import FlowableNode
# from rxbp.flowabletree.sources.connectable import ConnectableFlowableNode


#
# @dataclass
# class StateImpl(AsyncShareFlowableState, ShareFlowableState, State):
#     subscription_trampoline: Trampoline
#     default_scheduler: Scheduler
#     lambda_trace_logger: Logger
#     rxbp_observer: Observer | None  # pyright: ignore[reportMissingTypeArgument]
#     share_flowable_count: dict[ShareFlowableNode[ShareFlowableState, Any], int]  # pyright: ignore[reportExplicitAny]
#     share_virtual_count: dict[ShareFlowableNode[ShareFlowableState, Any], int]  # pyright: ignore[reportExplicitAny]
#     share_observers: dict[ShareFlowableNode[ShareFlowableState, Any], SharedObserver[Any]]  # pyright: ignore[reportExplicitAny]
#     async_share_observers_lock: Lock
#     async_share_observers: dict[AsyncShareKey[Self, Any], AsyncShareObserver[Any]]  # pyright: ignore[reportExplicitAny]
#
#
#     @property
#     @override
#     def p_subscription_trampoline(self) -> Trampoline:
#         return self.subscription_trampoline
#
#     @property
#     @override
#     def p_default_scheduler(self) -> Scheduler:
#         return self.default_scheduler
#
#     @property
#     @override
#     def p_lambda_trace_logger(self) -> Logger:
#         return self.lambda_trace_logger
#
#     @override
#     def update_subscription_trampoline(self, trampoline: Trampoline) -> Self:
#         return replace(self, subscription_trampoline=trampoline)
#
#     @property
#     @override
#     def p_rxbp_observer(self) -> Observer | None:  # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
#         return self.rxbp_observer  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]
#
#     @override
#     def update_rxbp_observer(self, observer: Observer | None) -> Self:  # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
#         return replace(self, rxbp_observer=observer)
#
#     @property
#     @override
#     def p_share_flowable_count(self) -> dict[ShareFlowableNode[ShareFlowableState, Any], int]:  # pyright: ignore[reportExplicitAny]
#         return self.share_flowable_count
#
#     @property
#     @override
#     def p_share_virtual_count(self) -> dict[ShareFlowableNode[ShareFlowableState, Any], int]:   # pyright: ignore[reportExplicitAny]
#         return self.share_virtual_count
#
#     @property
#     @override
#     def p_share_observers(
#         self,
#     ) -> dict[ShareFlowableNode[ShareFlowableState, Any], SharedObserver[Any]]:   # pyright: ignore[reportExplicitAny]
#         return self.share_observers
#
#     @property
#     @override
#     def p_async_share_observers_lock(self) -> Lock:
#         return self.async_share_observers_lock
#
#     @property
#     @override
#     def p_async_share_observers(self) -> dict[AsyncShareKey[Self, Any], AsyncShareObserver[Any]]:   # pyright: ignore[reportExplicitAny]
#         return self.async_share_observers
#
#     @override
#     def update_shared_flowable_data[U](
#         self,
#         flowable_count: dict[ShareFlowableNode[ShareFlowableState, U], int] | None = None,
#         virtual_count: dict[ShareFlowableNode[ShareFlowableState, U], int] | None = None,
#         share_observers: tuple[ShareFlowableNode[ShareFlowableState, U], SharedObserver[U]] | None = None,
#     ) -> ShareFlowableState:
#         def gen_args():
#             if flowable_count is not None:
#                 yield 'share_flowable_count', flowable_count
#             if virtual_count is not None:
#                 yield 'share_virtual_count', virtual_count
#             if share_observers is not None:
#                 n_share_observers = self.share_observers | {share_observers[0]: share_observers[1]}
#                 yield 'share_observers', n_share_observers
#
#         return replace(self, **dict(gen_args()))
#
#
# def run[U](
#     source: FlowableNode[State, U],
#     async_index: int | None = None,
#     lambda_trace_logger: Logger | None = None,
#     # scheduler: MainScheduler | None = None,
#     # connections: dict[ConnectableFlowableNode[S, U], FlowableNode[S, U]] | None = None,
# ):
#     if async_index is None:
#         async_index = 1
#
#     if lambda_trace_logger is None:
#         lambda_trace_logger = logging.getLogger("LambdaTrace")
#         lambda_trace_logger.setLevel(logging.DEBUG)
#
#     scheduler = contcert.init_contained_event_loop_scheduler(
#         lambda_trace_logger=lambda_trace_logger,
#     )
#     trampoline = contcert.init_trampoline_scheduler(
#
#         lambda_trace_logger=lambda_trace_logger,
#     )
#     # if scheduler is None:
#     #     scheduler = contcert.init_main_scheduler()
#     #
#     # subscribe_trampoline = contcert.init_trampoline()
#     virtual_count = 1
#
#     @dataclass()
#     class MainObserver(Observer[U]):
#         received_items: list[U]
#         received_exception: Exception | None
#         is_completed: bool
#
#         @override
#         def on_next(self, trampoline: Trampoline, item: U, observer: RxSingleObserver[None]) -> ContCert:
#             # print('on_next')
#             self.received_items.append(item)
#             return observer.on_success(trampoline, None)
#
#         @override
#         def on_next_and_complete(self, trampoline: Trampoline, item: U) -> ContCert:
#             # print('on_next_and_complete')
#             self.received_items.append(item)
#             self.is_completed = True
#             scheduler.stop()
#             cert = scheduler.acquire_certificate(
#                 virtual_count=virtual_count,
#                 stacks=tuple(),
#             )
#             # print(f'{cert=}')
#             return cert
#
#         @override
#         def on_completed(self) -> ContCert:
#             # print('on_completed()')
#             self.is_completed = True
#             scheduler.stop()
#             return scheduler.acquire_certificate(
#                 virtual_count=virtual_count,
#                 stacks=tuple(),
#             )
#
#         @override
#         def on_error(self, exception: Exception) -> ContCert:
#             self.received_exception = exception
#             self.is_completed = True
#             scheduler.stop()
#             return scheduler.acquire_certificate(
#                 virtual_count=virtual_count,
#                 stacks=tuple(),
#             )
#
#     # if connections is None:
#     #     connections = {}
#
#     observer = MainObserver(
#         received_items=[],
#         received_exception=None,
#         is_completed=False,
#     )
#
#     # def schedule_task():
#     #     def trampoline_task():
#     #         state = init_state(
#     #             subscription_trampoline=subscribe_trampoline,
#     #             # scheduler=scheduler,
#     #         )
#     #
#     #         return subscribe_single_sink(
#     #             source=source,
#     #             args=init_subscribe_args(
#     #                 observer=observer,
#     #                 virtual_count=1,
#     #                 scheduler=scheduler,
#     #             ),
#     #             state=state,
#     #             connections=connections,
#     #         )
#     #
#     #     # return result.certificate
#     #
#     #     return subscribe_trampoline.run(trampoline_task, virtual_count=1)
#     #
#     # scheduler.run(schedule_task, virtual_count=1, cancellation=None)
#
#
#     state = StateImpl(
#         # min_subscription_async_index=0,
#         lambda_trace_logger=lambda_trace_logger,
#         default_scheduler=scheduler,
#         subscription_trampoline=trampoline,
#         rxbp_observer=None,
#         share_flowable_count={},
#         share_virtual_count={},
#         share_observers={},
#         async_share_observers_lock=Lock(),
#         async_share_observers={},
#     )
#
#     # state = source.discover(state)
#
#     # async_index = state.min_subscription_context
#
#     def scheduler_callable():
#
#         def trampoline_callable() -> ContCert:
#             args = init_subscribe_args(
#                 observer=observer,
#                 scheduler=scheduler,
#                 virtual_count=virtual_count,
#                 async_index=context,
#             )
#
#             _, result = subscribe_single_sink(
#                 source=source,
#                 state=state,
#                 args=args,
#             )
#
#             return result.certificate
#
#         task = contcert.init_task(
#             callable=trampoline_callable,
#             virtual_count=virtual_count,
#             async_index=context,
#             cancellation=None,
#         )
#         return trampoline.run(task=task, stacks=tuple())
#
#     task = contcert.init_task(
#         callable=scheduler_callable,
#         virtual_count=virtual_count,
#         async_index=context,
#         cancellation=None,
#     )
#
#     _ = scheduler.run(
#         task=task,
#     )
#
#     assert observer.is_completed
#
#     if observer.received_exception:
#         raise observer.received_exception
#
#     return observer.received_items


# def to_rx[U](source: FlowableNode[S, U]) -> RxObservable[U]:
#     def subscribe(
#         observer: RxObserver[U],
#         scheduler,
#     ):
#         if scheduler is None:
#             from_rx_scheduler = None
#
#         else:
#
#             @dataclass
#             class FromRxScheduler(Scheduler):
#                 lock: RLock
#
#                 def schedule(
#                     self,
#                     task: Callable[[], ContCert],
#                     virtual_count: int,
#                     cancellation: Cancellation | None = None,
#                 ) -> ContCert:
#                     def action(_):
#                         task()
#                         return reactivex.disposable.Disposable()
#
#                     scheduler.schedule(action)
#
#                     return subscribe_trampoline._create_certificate(
#                         virtual_count=virtual_count, stack=tuple()
#                     )
#
#                 def schedule_relative(
#                     self,
#                     duetime: float,
#                     task: Callable[[], ContCert],
#                     virtual_count: int,
#                     cancellation: Cancellation | None = None,
#                 ) -> ContCert:
#                     def action(_):
#                         task()
#                         return reactivex.disposable.Disposable()
#
#                     scheduler.schedule_relative(duetime, action)
#
#                     return subscribe_trampoline._create_certificate(
#                         virtual_count=virtual_count, stack=tuple()
#                     )
#
#             from_rx_scheduler = FromRxScheduler(lock=RLock())
#         subscribe_trampoline = contcert.init_trampoline()
#
#         @dataclass()
#         class RxBPObserver(Observer[U]):
#             def on_next(self, value: U):
#                 observer.on_next(value)
#                 return rxsingle.from_(None)
#
#             def on_next_and_complete(self, value: U):
#                 observer.on_next(value)
#                 observer.on_completed()
#
#                 certificate = subscribe_trampoline._create_certificate(
#                     virtual_count=1, stack=tuple()
#                 )
#                 return rxsingle.from_(certificate)
#
#             def on_completed(self):
#                 observer.on_completed()
#                 certificate = subscribe_trampoline._create_certificate(
#                     virtual_count=1, stack=tuple()
#                 )
#                 return rxsingle.from_(certificate)
#
#             def on_error(self, exception: Exception):
#                 observer.on_error(exception)
#                 certificate = subscribe_trampoline._create_certificate(
#                     virtual_count=1, stack=tuple()
#                 )
#                 return rxsingle.from_(certificate)
#
#         # def trampoline_task():
#         state = init_state(
#             subscription_trampoline=subscribe_trampoline,
#             # scheduler=from_rx_scheduler,
#         )
#
#         args = init_subscribe_args(
#             observer=RxBPObserver(),
#             virtual_count=1,
#             scheduler=from_rx_scheduler,
#         )
#
#         certificate = subscribe_single_sink_on_trampoline(
#             source=source,
#             args=args,
#             state=state,
#         )
#
#         # result = source.subscribe(
#         #     state=state,
#         #     args=SubscribeArgs(
#         #         observer=RxBPObserver(),
#         #         schedule_virtual_count=1,
#         #     ),
#         # )
#
#         # return result.certificate
#
#         cancellation = CancellationState()
#
#         # certificate = subscribe_trampoline.run(
#         #     trampoline_task, virtual_count=1, cancellation=cancellation
#         # )
#
#         return reactivex.disposable.Disposable(lambda: cancellation.cancel(certificate))
#
#     return reactivex.Observable(subscribe)
