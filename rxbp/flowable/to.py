from dataclasses import dataclass
from threading import RLock
from typing import Callable

import reactivex.disposable

import continuationmonad
from continuationmonad.typing import Scheduler, ContinuationCertificate, Cancellation

import reactivex

from dataclassabc import dataclassabc
from rxbp.cancellable import CancellationState
from rxbp.flowable.flowable import Flowable
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.state import init_state


def to_rx(source: Flowable):
    def subscribe(
        obv,
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
        class MainObserver[U](Observer[U]):
            def on_next(self, value: U):
                # print(f'on_next{value}')
                obv.on_next(value)
                return continuationmonad.from_(None)

            def on_next_and_complete(self, value: U):
                obv.on_next(value)
                obv.on_completed()

                certificate = subscribe_trampoline._create_certificate(
                    weight=1, stack=tuple()
                )
                return continuationmonad.from_(certificate)
                # return main_trampoline.stop()

            def on_completed(self):
                obv.on_completed()
                certificate = subscribe_trampoline._create_certificate(
                    weight=1, stack=tuple()
                )
                return continuationmonad.from_(certificate)

            def on_error(self, exception: Exception):
                obv.on_error(exception)
                certificate = subscribe_trampoline._create_certificate(
                    weight=1, stack=tuple()
                )
                return continuationmonad.from_(certificate)

        observer = MainObserver()

        def trampoline_task():
            state = init_state(
                subscription_trampoline=subscribe_trampoline,
                scheduler=from_rx_scheduler,
            )

            result = source.subscribe(
                state=state,
                args=SubscribeArgs(
                    observer=observer,
                    schedule_weight=1,
                ),
            )

            return result.certificate

        cancellation = CancellationState()

        certificate = subscribe_trampoline.run(
            trampoline_task, weight=1, cancellation=cancellation
        )

        return reactivex.disposable.Disposable(lambda: cancellation.cancel(certificate))

    return reactivex.Observable(subscribe)
