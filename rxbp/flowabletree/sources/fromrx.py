from typing import override

from dataclasses import dataclass

import reactivex

import rxsingle

from rxbp.cancellable import init_cancellation_state
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.flowablenode import FlowableNode


@dataclass(frozen=True)
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
                        virtual_count=args.virtual_count,
                        stack=tuple()
                    )

                scheduler.schedule(
                    task=task,
                    virtual_count=1,
                    cancellation=cancellable,
                )

                return reactivex.disposable.Disposable()

        class RxObserver:
            def on_next(self, value):
                # print(f'on_next({value})')

                certificate = state.subscription_trampoline._create_certificate(
                    virtual_count=args.virtual_count,
                    stack=tuple()
                )

                trampoline = contcert.init_trampoline()

                def trampoline_task():
                    return args.observer.on_next(value).subscribe(
                        rxsingle.init_subscribe_args(
                            observer=rxsingle.init_anonymous_observer(
                                on_success=lambda _, __: certificate,
                            ),
                            virtual_count=args.virtual_count,
                            trampoline=trampoline,
                        )
                    )

                trampoline.start_loop(trampoline_task, virtual_count=args.virtual_count, cancellation=None)

            def on_error(self, error):
                trampoline = contcert.init_trampoline()

                def trampoline_task():
                    return args.observer.on_error(error).subscribe(
                        rxsingle.init_subscribe_args(
                            observer=rxsingle.init_anonymous_observer(
                                on_success=lambda _, certificate: certificate,
                            ),
                            virtual_count=args.virtual_count,
                            trampoline=state.subscription_trampoline,
                        )
                    )

                trampoline.start_loop(trampoline_task, virtual_count=args.virtual_count, cancellation=None)

            def on_completed(self):
                trampoline = contcert.init_trampoline()

                def trampoline_task():
                    return args.observer.on_completed().subscribe(
                        rxsingle.init_subscribe_args(
                            observer=rxsingle.init_anonymous_observer(
                                on_success=lambda _, certificate: certificate,
                            ),
                            virtual_count=args.virtual_count,
                            trampoline=state.subscription_trampoline,
                        )
                    )

                trampoline.start_loop(trampoline_task, virtual_count=args.virtual_count, cancellation=None)

        observer = RxObserver()

        def trampoline_task():
            self.source.subscribe(
                on_next=observer.on_next,
                on_error=observer.on_error,
                on_completed=observer.on_completed,
                scheduler=RxScheduler(),
            )

            return state.subscription_trampoline._create_certificate(
                virtual_count=args.virtual_count,
                stack=tuple()
            )

        certificate = state.subscription_trampoline.schedule(
            task=trampoline_task,
            virtual_count=args.virtual_count,
            cancellation=None,
        )

        return state, SubscriptionResult(
            certificate=certificate,
            cancellable=cancellable,
        )
