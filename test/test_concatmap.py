from unittest import TestCase

from donotation import do

import continuationmonad

import rxbp
from rxbp.state import init_state
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.operations.concatmap.flowable import ConcatMapFlowable
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.testing.tobserver import init_test_observer


class TestConcatMap(TestCase):

    def test_1(self):
        scheduler = continuationmonad.init_virtual_time_scheduler()

        @do()
        def schedule_inner_source1(observer: Observer, _):
            yield continuationmonad.delay(scheduler, 1)
            _ = yield observer.on_next(1)
            yield continuationmonad.delay(scheduler, 1)
            return observer.on_next_and_complete(2)

        inner_source1 = rxbp.create(schedule_inner_source1)

        @do()
        def schedule_inner_source2(observer: Observer, _):
            yield continuationmonad.delay(scheduler, 1)
            _ = yield observer.on_next(3)
            yield continuationmonad.delay(scheduler, 1)
            return observer.on_next_and_complete(4)
        inner_source2 = rxbp.create(schedule_inner_source2)

        @do()
        def schedule_source(observer: Observer, _):
            yield observer.on_next(inner_source1)
            return observer.on_next_and_complete(inner_source2)

        source = rxbp.create(schedule_source)

        sink = init_test_observer()

        state = init_state(
            scheduler=scheduler
        )

        def trampoline_task(state=state):

            state, result = ConcatMapFlowable(
                child=source,
                func=lambda v: v,
            ).unsafe_subscribe(state, SubscribeArgs(
                observer=sink,
                weight=1,
            ))
            return result.certificate
        
        certificate = state.subscription_trampoline.run(trampoline_task, weight=1)

        sink.certificate = certificate

        scheduler.advance_to(1.5)
        self.assertEqual(sink.received, [1])

        scheduler.advance_to(2.5)
        self.assertEqual(sink.received, [1, 2])
        self.assertFalse(sink.is_completed)

        scheduler.advance_to(3.5)
        self.assertEqual(sink.received, [1, 2, 3])

        scheduler.advance_to(4.5)
        self.assertEqual(sink.received, [1, 2, 3, 4])
        self.assertTrue(sink.is_completed)

