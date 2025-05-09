from continuationmonad.typing import Scheduler

import rxbp
from rxbp.typing import FlowableNode
from rxbp.testing.tobserver import TObserver


def test_run(
    source: FlowableNode,
    sink: TObserver,
    scheduler: Scheduler,
):

    state = rxbp.init_state(
        scheduler=scheduler
    )

    def trampoline_task(state=state):

        state, result = source.unsafe_subscribe(
            state, 
            rxbp.init_subscribe_args(
                observer=sink,
                weight=1,
            ),
        )
        return result.certificate
    
    certificate = state.subscription_trampoline.run(trampoline_task, weight=1)

    sink.certificate = certificate
