from dataclasses import dataclass

from continuationmonad.typing import MainVirtualTimeScheduler, ContinuationCertificate

import rxbp
from rxbp.flowabletree.subscribeandconnect import subscribe_and_connect
from rxbp.flowabletree.subscription import Subscription
from rxbp.typing import FlowableNode, State
from rxbp.testing.tobserver import TObserver


def test_run(
    source: FlowableNode,
    sinks: tuple[TObserver, ...],
    scheduler: MainVirtualTimeScheduler,
    state: State | None = None
):

    if state is None:
        state = rxbp.init_state()

    def scheduler_task(state=state):
        def trampoline_task(state=state):

            def gen_subscriptions():
                for sink in sinks:
                    @dataclass
                    class TestSubscription(Subscription):
                        sink: TObserver
                        certificate: ContinuationCertificate

                        def discover(self, state: State):
                            return source.discover(state)

                        def assign_weights(self, state: State, weight: int):
                            return source.assign_weights(state, weight)

                        def apply(self, state: State):
                            state, result = source.unsafe_subscribe(
                                state,
                                args=rxbp.init_subscribe_args(
                                    observer=self.sink,
                                    weight=1,
                                    scheduler=scheduler
                                ),
                            )

                            self.certificate = result.certificate
                            self.sink.cancellable = result.cancellable

                            return state
                        
                    yield TestSubscription(
                        sink=sink,
                        certificate=None, # type: ignore
                    )

            subscriptions = tuple(gen_subscriptions())

            state = subscribe_and_connect(
                subscriptions=subscriptions,
                state=state,
            )

            return ContinuationCertificate.merge(tuple(
                s.certificate for s in subscriptions
            ))
        
        return state.subscription_trampoline.start_loop(
            trampoline_task, weight=len(sinks), cancellation=None,
        )
    
    scheduler.run(scheduler_task, weight=len(sinks))
    #     return certificate

    # certificate = state.subscription_trampoline.start_loop(
    #     trampoline_task, weight=len(sinks), cancellation=None,
    # )

    # for sink in sinks:
    #     sink.certificate, certificate = certificate.take(1)

    return state
