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


class TestMap(TestCase):
    def test_map_completed(self):
        subscription = [None]
        s = [None]
        scheduler = TestScheduler()

        xs = BPHotObservable(scheduler, [
            on_next(220, 3),
            on_next(230, 3),
        ])

        observer = BackpressureMockObserver(
            scheduler, [
             Recorded(210, 1),
            ]
        )

        def action1(scheduler, state=None):
            s[0] = xs.first()
        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            subscription[0] = s[0].subscribe(observer=observer)
        scheduler.schedule_absolute(200, action2)
        scheduler.start()

        print(observer.messages)

        observer.messages.assert_equal(
            on_next(220, 3),
            on_completed(220),
        )

        # results1.bp_messages.assert_equal(
        #     bp_response(420, 1),
        #     bp_response(430, 1),
        # )
        #
        # xs.subscriptions.assert_equal(subscribe(200, 430))