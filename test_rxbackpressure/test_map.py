import types
from unittest import TestCase

from rx.testing import TestScheduler, ReactiveTest
from rx.testing.recorded import Recorded

from rxbackpressure.testing.backpressuremockobserver import BackpressureMockObserver
from rxbackpressure.testing.notification import BPResponse

on_next = ReactiveTest.on_next
on_completed = ReactiveTest.on_completed
on_error = ReactiveTest.on_error
subscribe = ReactiveTest.subscribe
subscribed = ReactiveTest.subscribed
disposed = ReactiveTest.disposed
created = ReactiveTest.created


class BPResponsePredicate(object):
    def __init__(self, predicate):
        self.predicate = predicate

    def __eq__(self, other):
        if other == self:
            return True
        if other is None:
            return False
        if other.kind != 'B':
            return False
        return self.predicate(other.number_of_items)

def bp_response(ticks, value):
    if isinstance(value, types.FunctionType):
        return Recorded(ticks, BPResponsePredicate(value))

    return Recorded(ticks, BPResponse(value))

class TestSubscriptionBase(TestCase):
    # def test_select_completed(self):
    #     scheduler = TestScheduler()
    #
    #     xs = scheduler.create_hot_observable(on_next(180, 1), on_next(210, 2), on_next(240, 3), on_next(290, 4),
    #                                          on_next(350, 5), on_completed(400), on_next(410, -1), on_completed(420),
    #                                          on_error(430, 'ex'))
    #     invoked = [0]
    #
    #     def factory():
    #         def projection(x, i):
    #             invoked[0] += 1
    #             return x + 1
    #
    #         return xs.to_backpressure().map(projection).to_observable()
    #
    #     results = scheduler.start(factory)
    #
    #     results.messages.assert_equal(on_next(200, 2), on_next(210, 3), on_next(240, 4), on_next(290, 5), on_next(350, 6),
    #                                   on_completed(400))
    #     # xs.subscriptions.assert_equal(ReactiveTest.subscribe(200, 400))
    #
    #     assert invoked[0] == 5

    def test_map_completed(self):
        subscription = [None]
        s = [None]
        scheduler = TestScheduler()

        xs = scheduler.create_hot_observable(
            on_next(70, 1),
            on_next(110, 2),
            on_next(420, 3),
            on_completed(430),
        )

        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(150, 1),
             Recorded(260, 1),
             Recorded(310, 1),
             Recorded(410, 1),
            ]
        )

        def action1(scheduler, state=None):
            s[0] = xs.to_backpressure().map(lambda v, idx: v+100)
        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            subscription[0] = s[0].subscribe(observer=results1, subscribe_bp=results1.subscribe_backpressure)
        scheduler.schedule_absolute(200, action2)
        scheduler.start()

        # dispose
        def action3(scheduler, state=None):
            subscription[0].dispose()
        scheduler.schedule_absolute(600, action3)

        results1.messages.assert_equal(
            on_next(260, 102),
            bp_response(260, 1),
            on_next(420, 103),
            bp_response(420, 1),
            on_completed(430),
            bp_response(430, 1),
        )