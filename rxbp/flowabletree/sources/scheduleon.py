from typing import override

from dataclassabc import dataclassabc

import continuationmonad
from continuationmonad.typing import Scheduler

from rxbp.cancellable import init_cancellation_state
from rxbp.state import State
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult
from rxbp.flowabletree.nodes import FlowableNode


@dataclassabc(frozen=True)
class ScheduleOnFlowable(FlowableNode[Scheduler]):
    scheduler: Scheduler

    @override
    def unsafe_subscribe(
        self,
        state: State,
        args: SubscribeArgs[Scheduler],
    ) -> tuple[State, SubscriptionResult]:
        cancellable = init_cancellation_state()

        certificate = continuationmonad.fork(
            source=(
                continuationmonad.schedule_on(self.scheduler)
                .flat_map(lambda _: args.observer.on_next_and_complete(self.scheduler))
            ),
            scheduler=state.subscription_trampoline,  # ensures scheduling on trampoline
            cancellation=cancellable,
            weight=args.schedule_weight,
        )

        result = SubscriptionResult(
            certificate=certificate,
            cancellable=cancellable,
        )

        return state, result


def init_schedule_on(scheduler: Scheduler):
    return ScheduleOnFlowable(scheduler=scheduler)
