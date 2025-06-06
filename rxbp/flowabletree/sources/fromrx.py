from typing import override

from dataclassabc import dataclassabc

import reactivex

import continuationmonad

from rxbp.cancellable import init_cancellation_state
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode


@dataclassabc(frozen=True)
class FromRx[V](FlowableNode[V]):
    source: None

    @override
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[V],
    ):
        
        if args.scheduler is None:
            scheduler = state.subscription_trampoline
        else:
            scheduler = args.scheduler

        cancellable = init_cancellation_state()

        class RxScheduler:
            def schedule(self, action):
                def task():
                    action(None)
                    return state.subscription_trampoline._create_certificate(
                        weight=args.weight,
                        stack=tuple()
                    )

                scheduler.schedule(
                    task=task,
                    weight=1,
                    cancellation=cancellable,
                )

                return reactivex.disposable.Disposable()

        class RxObserver:
            def on_next(self, value):
                # print(f'on_next({value})')

                certificate = state.subscription_trampoline._create_certificate(
                    weight=args.weight,
                    stack=tuple()
                )

                trampoline = continuationmonad.init_trampoline()

                def trampoline_task():
                    return args.observer.on_next(value).subscribe(
                        continuationmonad.init_subscribe_args(
                            observer=continuationmonad.init_anonymous_observer(
                                on_success=lambda _, __: certificate,
                            ),
                            weight=args.weight,
                            trampoline=trampoline,
                        )
                    )

                trampoline.start_loop(trampoline_task, weight=args.weight, cancellation=None)

            def on_error(self, error):
                trampoline = continuationmonad.init_trampoline()

                def trampoline_task():
                    return args.observer.on_error(error).subscribe(
                        continuationmonad.init_subscribe_args(
                            observer=continuationmonad.init_anonymous_observer(
                                on_success=lambda _, certificate: certificate,
                            ),
                            weight=args.weight,
                            trampoline=state.subscription_trampoline,
                        )
                    )

                trampoline.start_loop(trampoline_task, weight=args.weight, cancellation=None)

            def on_completed(self):
                trampoline = continuationmonad.init_trampoline()

                def trampoline_task():
                    return args.observer.on_completed().subscribe(
                        continuationmonad.init_subscribe_args(
                            observer=continuationmonad.init_anonymous_observer(
                                on_success=lambda _, certificate: certificate,
                            ),
                            weight=args.weight,
                            trampoline=state.subscription_trampoline,
                        )
                    )

                trampoline.start_loop(trampoline_task, weight=args.weight, cancellation=None)

        observer = RxObserver()

        def trampoline_task():
            self.source.subscribe(
                on_next=observer.on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=RxScheduler(),
            )

            return state.subscription_trampoline._create_certificate(
                weight=args.weight,
                stack=tuple()
            )

        certificate = state.subscription_trampoline.schedule(
            task=trampoline_task,
            weight=args.weight,
            cancellation=None,
        )

        return state, SubscriptionResult(
            certificate=certificate,
            cancellable=cancellable,
        )
