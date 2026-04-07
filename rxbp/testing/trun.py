from dataclasses import dataclass

from rxsingle.typing import MainVirtualTimeScheduler, ContCert

import rxbp
from rxbp.flowabletree.subscribe import subscribe
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
                        certificate: ContCert

                        def discover(self, state: State):
                            return source.discover(state)

                        def assign_virtual_count(self, state: State, virtual_count: int):
                            return source.assign_virtual_count(state, virtual_count)

                        def apply(self, state: State):
                            state, result = source.unsafe_subscribe(
                                state,
                                args=rxbp.init_subscribe_args(
                                    observer=self.sink,
                                    virtual_count=1,
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

            state = subscribe(
                subscriptions=subscriptions,
                state=state,
            )

            return ContCert.merge(tuple(
                s.certificate for s in subscriptions
            ))
        
        return state.subscription_trampoline.start_loop(
            trampoline_task, virtual_count=len(sinks), cancellation=None,
        )
    
    # using run, we enforce that `stop()` is called
    scheduler.run(scheduler_task, virtual_count=len(sinks))

    #     return certificate

    # certificate = state.subscription_trampoline.start_loop(
    #     trampoline_task, virtual_count=len(sinks), cancellation=None,
    # )

    # for sink in sinks:
    #     sink.certificate, certificate = certificate.take(1)

    return state
