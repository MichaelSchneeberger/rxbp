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


class TestSubscriptionBase(TestCase):
    def test_map_completed(self):
        subscription = [None]
        s = [None]
        scheduler = TestScheduler()

        # xs = scheduler.create_hot_observable(
        messages = [
            on_next(70, 1),
            on_next(110, 2),
            on_next(420, 3),
            on_completed(430),
            on_next(670, 4),
            on_completed(690),
        ]
        xs = BPHotObservable(scheduler, messages)

        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(150, 1),
             Recorded(260, 1),
             Recorded(310, 1),
             Recorded(410, 1),
            ]
        )

        def action1(scheduler, state=None):
            s[0] = xs.map(lambda v: v+100)
        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            subscription[0] = s[0].subscribe(observer=results1, subscribe_bp=results1.subscribe_backpressure)
        scheduler.schedule_absolute(200, action2)
        scheduler.start()

        # dispose
        def action3(scheduler, state=None):
            print('dispose')
            subscription[0].dispose()
        scheduler.schedule_absolute(600, action3)

        print(results1.bp_messages)

        results1.messages.assert_equal(
            on_next(420, 103),
            on_completed(430),
        )

        results1.bp_messages.assert_equal(
            bp_response(420, 1),
            bp_response(430, 1),        # why?
        )

        xs.subscriptions.assert_equal(subscribe(200, 430))