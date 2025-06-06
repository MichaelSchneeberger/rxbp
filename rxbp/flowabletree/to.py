from dataclasses import dataclass
from threading import RLock
from typing import Callable

from dataclassabc import dataclassabc

import reactivex
import reactivex.disposable
from reactivex import (
    Observer as RxObserver,
    Observable as RxObservable,
)

import continuationmonad
from continuationmonad.typing import (
    Scheduler,
    ContinuationCertificate,
    Cancellation,
    MainScheduler,
)

from rxbp.cancellable import CancellationState
from rxbp.flowabletree.subscribeargs import init_subscribe_args
from rxbp.state import init_state
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.sources.connectable import ConnectableFlowableNode
from rxbp.flowabletree.subscribeandconnect import subscribe_single_sink_on_trampoline


def run[U](
    source: FlowableNode[U],
    scheduler: MainScheduler | None = None,
    connections: dict[ConnectableFlowableNode, FlowableNode] | None = None,
):
    if scheduler is None:
        scheduler = continuationmonad.init_main_scheduler()

    subscribe_trampoline = continuationmonad.init_trampoline()

    @dataclass()
    class MainObserver(Observer[U]):
        received_items: list[U]
        received_exception: list[Exception | None]
        is_completed: bool

        def on_next(self, value: U):
            self.received_items.append(value)
            return continuationmonad.from_(None)

        def on_next_and_complete(self, value: U):
            self.received_items.append(value)
            self.is_completed = True
            return continuationmonad.from_(scheduler.stop())

        def on_completed(self):
            self.is_completed = True
            return continuationmonad.from_(scheduler.stop())

        def on_error(self, exception: Exception):
            self.received_exception[0] = exception
            self.is_completed = True
            return continuationmonad.from_(scheduler.stop())

    if connections is None:
        connections = {}

    observer = MainObserver(
        received_items=[],
        received_exception=[None],
        is_completed=False,
    )

    def schedule_task():
        # def trampoline_task():
        state = init_state(
            subscription_trampoline=subscribe_trampoline,
            # scheduler=scheduler,
        )

        return subscribe_single_sink_on_trampoline(
            source=source,
            args=init_subscribe_args(
                observer=observer,
                weight=1,
                scheduler=scheduler,
            ),
            state=state,
            connections=connections,
        )

        # return result.certificate

        # return subscribe_trampoline.run(trampoline_task, weight=1)

    scheduler.run(schedule_task, weight=1, cancellation=None)

    assert observer.is_completed

    if observer.received_exception[0]:
        raise observer.received_exception[0]

    return observer.received_items


def to_rx[U](source: FlowableNode[U]) -> RxObservable[U]:
    def subscribe(
        observer: RxObserver[U],
        scheduler,
    ):
        if scheduler is None:
            from_rx_scheduler = None

        else:

            @dataclassabc
            class FromRxScheduler(Scheduler):
                lock: RLock

                def schedule(
                    self,
                    task: Callable[[], ContinuationCertificate],
                    weight: int,
                    cancellation: Cancellation | None = None,
                ) -> ContinuationCertificate:
                    def action(_):
                        task()
                        return reactivex.disposable.Disposable()

                    scheduler.schedule(action)

                    return subscribe_trampoline._create_certificate(
                        weight=weight, stack=tuple()
                    )

                def schedule_relative(
                    self,
                    duetime: float,
                    task: Callable[[], ContinuationCertificate],
                    weight: int,
                    cancellation: Cancellation | None = None,
                ) -> ContinuationCertificate:
                    def action(_):
                        task()
                        return reactivex.disposable.Disposable()

                    scheduler.schedule_relative(duetime, action)

                    return subscribe_trampoline._create_certificate(
                        weight=weight, stack=tuple()
                    )

            from_rx_scheduler = FromRxScheduler(lock=RLock())
        subscribe_trampoline = continuationmonad.init_trampoline()

        @dataclass()
        class RxBPObserver(Observer[U]):
            def on_next(self, value: U):
                observer.on_next(value)
                return continuationmonad.from_(None)

            def on_next_and_complete(self, value: U):
                observer.on_next(value)
                observer.on_completed()

                certificate = subscribe_trampoline._create_certificate(
                    weight=1, stack=tuple()
                )
                return continuationmonad.from_(certificate)

            def on_completed(self):
                observer.on_completed()
                certificate = subscribe_trampoline._create_certificate(
                    weight=1, stack=tuple()
                )
                return continuationmonad.from_(certificate)

            def on_error(self, exception: Exception):
                observer.on_error(exception)
                certificate = subscribe_trampoline._create_certificate(
                    weight=1, stack=tuple()
                )
                return continuationmonad.from_(certificate)

        # def trampoline_task():
        state = init_state(
            subscription_trampoline=subscribe_trampoline,
            # scheduler=from_rx_scheduler,
        )

        args = init_subscribe_args(
            observer=RxBPObserver(),
            weight=1,
            scheduler=from_rx_scheduler,
        )

        certificate = subscribe_single_sink_on_trampoline(
            source=source,
            args=args,
            state=state,
        )

        # result = source.subscribe(
        #     state=state,
        #     args=SubscribeArgs(
        #         observer=RxBPObserver(),
        #         schedule_weight=1,
        #     ),
        # )

            # return result.certificate

        cancellation = CancellationState()

        # certificate = subscribe_trampoline.run(
        #     trampoline_task, weight=1, cancellation=cancellation
        # )

        return reactivex.disposable.Disposable(lambda: cancellation.cancel(certificate))

    return reactivex.Observable(subscribe)
