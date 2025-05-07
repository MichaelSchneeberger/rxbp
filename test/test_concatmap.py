from unittest import TestCase

from donotation import do

import continuationmonad

from rxbp.state import init_state
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.concatmap.flowable import ConcatMapFlowable
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.testing.tflowable import init_test_flowable
from rxbp.testing.tobserver import init_test_observer


class TestConcatMap(TestCase):

    def test_1(self):
        scheduler = continuationmonad.init_virtual_time_scheduler()

        @do()
        def schedule_inner_source1(observer: Observer):
            # print('start inner')
            yield continuationmonad.schedule_relative(scheduler, 1)
            _ = yield observer.on_next(1)
            yield continuationmonad.schedule_relative(scheduler, 1)
            # _ = yield observer.on_next(2)
            return observer.on_next_and_complete(2)

        inner_source1 = init_test_flowable(schedule_inner_source1)

        @do()
        def schedule_source(observer: Observer):
            # print('send inner source')
            yield observer.on_next(inner_source1)
            return observer.on_completed()

        source = init_test_flowable(schedule_source)

        sink = init_test_observer()

        state = init_state()

        def trampoline_task(state=state):

            state, result = ConcatMapFlowable(
                child=source,
                func=lambda v: v,
            ).unsafe_subscribe(state, SubscribeArgs(
                observer=sink,
                schedule_weight=1,
            ))
            return result.certificate
        
        certificate = state.subscription_trampoline.run(trampoline_task, weight=1)

        sink.certificate = certificate

        scheduler.advance_to(1.5)
        self.assertEqual(sink.received, [1])

        scheduler.advance_to(2.5)
        self.assertEqual(sink.received, [1, 2])

