from unittest import TestCase

from rx.testing import TestScheduler, ReactiveTest
from rx.testing.recorded import Recorded

from rxbackpressure.testing.backpressuremockobserver import BackpressureMockObserver
from rxbackpressure.testing.bphotobservable import BPHotObservable
from rxbackpressure.testing.notification import bp_response

on_next = ReactiveTest.on_next
on_completed = ReactiveTest.on_completed
on_error = ReactiveTest.on_error
subscribe = ReactiveTest.subscribe
subscribed = ReactiveTest.subscribed
disposed = ReactiveTest.disposed
created = ReactiveTest.created


class TestFlatMap(TestCase):
    def test1(self):
        subscription = [None]
        s = [None]
        scheduler = TestScheduler()

        child1 = BPHotObservable(scheduler,
            [
            on_next(230, 1),
            on_next(250, 2),
            on_completed(280),
        ])

        child2 = BPHotObservable(scheduler,
            [
            on_next(330, 10),
            on_next(350, 11),
            on_completed(390),
        ])

        parent_obs = BPHotObservable(scheduler,
            [
            on_next(220, child1),
            on_next(320, child2),
            on_completed(430),
        ])

        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(260, 1),
             Recorded(270, 1),
             Recorded(310, 1),
             Recorded(340, 1),
             Recorded(380, 1),
            ]
        )

        def action1(scheduler, state=None):
            s[0] = parent_obs.flat_map(lambda v: v)
        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            subscription[0] = s[0].subscribe(observer=results1, subscribe_bp=results1.subscribe_backpressure)
        scheduler.schedule_absolute(200, action2)
        scheduler.start()

        results1.messages.assert_equal(
            on_next(260, 1),
            on_next(270, 2),
            on_next(330, 10),
            on_next(350, 11),
            on_completed(430),
        )

        results1.bp_messages.assert_equal(
            bp_response(260, 1),
            bp_response(270, 1),
            bp_response(330, 1),
            bp_response(350, 1),
            bp_response(430, 0),
        )

        # xs.subscriptions.assert_equal(subscribe(200, 430))

TestFlatMap().test1()