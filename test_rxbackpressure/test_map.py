from unittest import TestCase

from rx.testing import TestScheduler, ReactiveTest
from rx.testing.recorded import Recorded

from rxbackpressure.subjects.bufferedsubject import BufferedSubject
from rxbackpressure.testing.backpressuremockobserver import BackpressureMockObserver

on_next = ReactiveTest.on_next
on_completed = ReactiveTest.on_completed
on_error = ReactiveTest.on_error
subscribe = ReactiveTest.subscribe
subscribed = ReactiveTest.subscribed
disposed = ReactiveTest.disposed
created = ReactiveTest.created

class TestSubscriptionBase(TestCase):
    def test_select_completed(self):
        scheduler = TestScheduler()

        xs = scheduler.create_hot_observable(on_next(180, 1), on_next(210, 2), on_next(240, 3), on_next(290, 4),
                                             on_next(350, 5), on_completed(400), on_next(410, -1), on_completed(420),
                                             on_error(430, 'ex'))
        invoked = [0]

        def factory():
            def projection(x, i):
                invoked[0] += 1
                return x + 1

            return xs.to_backpressure().map(projection).to_observable()

        results = scheduler.start(factory)

        results.messages.assert_equal(on_next(200, 2), on_next(210, 3), on_next(240, 4), on_next(290, 5), on_next(350, 6),
                                      on_completed(400))
        # xs.subscriptions.assert_equal(ReactiveTest.subscribe(200, 400))

        assert invoked[0] == 5