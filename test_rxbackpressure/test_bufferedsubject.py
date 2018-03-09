from unittest import TestCase

import math

from rx.backpressure.controlledsubject import ControlledSubject
from rx.testing import TestScheduler, ReactiveTest
from rx.testing.recorded import Recorded

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.subjects.bufferedsubject import BufferedSubject
from rxbackpressure.testing.backpressuremockobserver import BackpressureMockObserver
from rxbackpressure.testing.notification import bp_response

on_next = ReactiveTest.on_next
on_completed = ReactiveTest.on_completed
on_error = ReactiveTest.on_error
subscribe = ReactiveTest.subscribe
subscribed = ReactiveTest.subscribed
disposed = ReactiveTest.disposed
created = ReactiveTest.created


class TestBufferedSubject(TestCase):
    # def test_request_one_element_in_infinit_sequence(self):
    #     subscription = [None]
    #     s = [None]
    #     scheduler = TestScheduler()
    #
    #     xs = scheduler.create_hot_observable(
    #         on_next(310, 1),
    #         on_next(320, 2),
    #         on_next(370, 3),
    #         on_next(380, 4),
    #     )
    #
    #     results1 = BackpressureMockObserver(
    #         scheduler,
    #         [Recorded(330, 1),
    #          Recorded(340, 1),
    #          Recorded(350, 1),
    #          Recorded(360, 1),
    #          ]
    #     )
    #
    #     # create
    #     def action1(scheduler, state=None):
    #         s[0] = BufferedSubject()
    #     scheduler.schedule_absolute(100, action1)
    #
    #     # subscribe
    #     def action2(scheduler, state=None):
    #         xs.subscribe(s[0])
    #     scheduler.schedule_absolute(200, action2)
    #
    #     def action4(scheduler, state=None):
    #         subscription[0] = s[0].subscribe(observer=results1, subscribe_bp=results1.subscribe_backpressure)
    #     scheduler.schedule_absolute(300, action4)
    #
    #     # dispose
    #     def action3(scheduler, state=None):
    #         subscription[0].dispose()
    #     scheduler.schedule_absolute(600, action3)
    #
    #     scheduler.start()
    #
    #     print(results1.messages)
    #     results1.messages.assert_equal(
    #         on_next(330, 1),
    #         on_next(340, 2),
    #         on_next(370, 3),
    #         on_next(380, 4),
    #     )

    # def test_request_one_element_in_finit_sequence(self):
    #     subscription = [None]
    #     s = [None]
    #     scheduler = TestScheduler()
    #
    #     xs = scheduler.create_hot_observable(
    #         on_next(310, 1),
    #         on_next(320, 2),
    #         on_completed(400),
    #     )
    #
    #     results1 = BackpressureMockObserver(
    #         scheduler,
    #         [Recorded(330, 1),
    #          Recorded(340, 1),
    #          Recorded(350, 1),
    #          Recorded(360, 1)]
    #     )
    #
    #     def action1(scheduler, state=None):
    #         s[0] = BufferedSubject(scheduler=scheduler)
    #     scheduler.schedule_absolute(100, action1)
    #
    #     def action2(scheduler, state=None):
    #         subscription[0] = xs.subscribe(s[0])
    #     scheduler.schedule_absolute(200, action2)
    #
    #     def action4(scheduler, state=None):
    #         subscription[0] = s[0].subscribe(observer=results1, subscribe_bp=results1.subscribe_backpressure)
    #     scheduler.schedule_absolute(300, action4)
    #     scheduler.start()
    #
    #     def action3(scheduler, state=None):
    #         subscription[0].dispose()
    #     scheduler.schedule_absolute(600, action3)
    #
    #     print(results1.messages)
    #     print(results1.bp_messages)
    #
    #     results1.messages.assert_equal(
    #         on_next(332, 1),
    #         on_next(342, 2),
    #         on_completed(402)
    #     )
    #
    #     results1.bp_messages.assert_equal(
    #         bp_response(332, 1),
    #         bp_response(342, 1),
    #         bp_response(402, 0),
    #     )

    def test_request_one_element_and_disposing_sequence(self):
        subscription = [None]
        s = [None]
        scheduler = TestScheduler()

        xs = scheduler.create_hot_observable(
            on_next(230, 2),
            # on_next(660, 5),
            # on_completed(710),
        )

        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(380, 1),
             Recorded(530, 1),
             Recorded(610, 1)]
        )

        def action1(scheduler, state=None):
            s[0] = BufferedSubject(scheduler=scheduler)
        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            subscription[0] = xs.subscribe(s[0])
        scheduler.schedule_absolute(200, action2)

        def action4(scheduler, state=None):
            subscription[0] = s[0].subscribe(observer=results1, subscribe_bp=results1.subscribe_backpressure)
        scheduler.schedule_absolute(300, action4)

        def action3(scheduler, state=None):
            subscription[0].dispose()
        scheduler.schedule_absolute(600, action3)

        scheduler.start()

        results1.messages.assert_equal(
            on_next(382, 2),
        )

        results1.bp_messages.assert_equal(
            bp_response(382, 1),
            bp_response(611, 0)
        )

    # def test_request_multible_elements(self):
    #     subscription = [None]
    #     s = [None]
    #     scheduler = TestScheduler()
    #
    #     xs = scheduler.create_hot_observable(
    #         on_next(70, 1),
    #         on_next(110, 2),
    #         on_next(220, 3),
    #         on_next(340, 4),
    #         on_next(420, 5),
    #         on_next(520, 6),
    #         on_next(630, 7),
    #     )
    #
    #     results1 = BackpressureMockObserver(
    #         scheduler,
    #         [Recorded(320, 3),
    #          Recorded(330, 1)]
    #     )
    #
    #     def action1(scheduler, state=None):
    #         s[0] = BufferedSubject()
    #     scheduler.schedule_absolute(100, action1)
    #
    #     def action2(scheduler, state=None):
    #         subscription[0] = xs.subscribe(s[0])
    #     scheduler.schedule_absolute(200, action2)
    #
    #     def action4(scheduler, state=None):
    #         subscription[0] = s[0].subscribe(observer=results1, subscribe_bp=results1.subscribe_backpressure)
    #     scheduler.schedule_absolute(300, action4)
    #
    #     def action3(scheduler, state=None):
    #         subscription[0].dispose()
    #     scheduler.schedule_absolute(600, action3)
    #
    #     scheduler.start()
    #
    #     print(results1.messages)
    #
    #     results1.messages.assert_equal(
    #         on_next(320, 3),
    #         on_next(340, 4),
    #         on_next(420, 5),
    #         on_next(520, 6),
    #     )
    #
    #     results1.bp_messages.assert_equal(
    #         bp_response(420, 3),
    #         bp_response(520, 1),
    #     )

    # def test_request_multible_elements_2(self):
    #     subscription = [None]
    #     subscription2 = [None]
    #     s = [None]
    #     scheduler = TestScheduler()
    #
    #     xs = scheduler.create_hot_observable(
    #         on_next(70, 1),
    #         on_next(110, 2),
    #         on_next(220, 3),
    #         on_next(340, 4),
    #         on_next(420, 5),
    #         on_next(520, 6),
    #         on_next(630, 7),
    #     )
    #
    #     results1 = BackpressureMockObserver(
    #         scheduler,
    #         [Recorded(320, 3),
    #          Recorded(330, 1)]
    #     )
    #
    #     results2 = BackpressureMockObserver(
    #         scheduler,
    #         [Recorded(320, 1),
    #          Recorded(560, 1)]
    #     )
    #
    #     def action1(scheduler, state=None):
    #         s[0] = BufferedSubject()
    #     scheduler.schedule_absolute(100, action1)
    #
    #     def action2(scheduler, state=None):
    #         subscription[0] = xs.subscribe(s[0])
    #     scheduler.schedule_absolute(200, action2)
    #
    #     def action4(scheduler, state=None):
    #         subscription[0] = s[0].subscribe(observer=results1, subscribe_bp=results1.subscribe_backpressure)
    #     scheduler.schedule_absolute(300, action4)
    #
    #     def action5(scheduler, state=None):
    #         subscription2[0] = s[0].subscribe(observer=results2, subscribe_bp=results2.subscribe_backpressure)
    #     scheduler.schedule_absolute(310, action5)
    #
    #     def action3(scheduler, state=None):
    #         subscription[0].dispose()
    #         subscription2[0].dispose()
    #     scheduler.schedule_absolute(600, action3)
    #
    #     scheduler.start()
    #
    #     results1.messages.assert_equal(
    #         on_next(320, 3),
    #         on_next(340, 4),
    #         on_next(420, 5),
    #         on_next(520, 6),
    #     )
    #
    #     results2.messages.assert_equal(
    #         on_next(320, 3),
    #         on_next(560, 4),
    #     )

    # def test_request_infinite(self):
    #     subscription = [None]
    #     subscription2 = [None]
    #     s = [None]
    #     scheduler = TestScheduler()
    #
    #     xs = scheduler.create_hot_observable(
    #         on_next(70, 1),
    #         on_next(110, 2),
    #         on_next(220, 3),
    #         on_next(340, 4),
    #         on_next(410, 5),
    #         on_next(420, 6),
    #     )
    #
    #     results1 = BackpressureMockObserver(
    #         scheduler,
    #         [Recorded(320, 1),
    #          Recorded(430, StopRequest())]
    #     )
    #
    #     def action1(scheduler, state=None):
    #         s[0] = BufferedSubject()
    #     scheduler.schedule_absolute(100, action1)
    #
    #     def action2(scheduler, state=None):
    #         subscription[0] = xs.subscribe(s[0])
    #     scheduler.schedule_absolute(200, action2)
    #
    #     def action4(scheduler, state=None):
    #         subscription[0] = s[0].subscribe(observer=results1, subscribe_bp=results1.subscribe_backpressure)
    #     scheduler.schedule_absolute(300, action4)
    #
    #     def action3(scheduler, state=None):
    #         subscription[0].dispose()
    #     scheduler.schedule_absolute(600, action3)
    #
    #     scheduler.start()
    #
    #     print(results1.messages)
    #     results1.messages.assert_equal(
    #         on_next(320, 3),
    #         on_completed(430),
    #     )
    #
    #     print([e.value for e in s[0].buffer.queue])
    #
    #     assert(len(s[0].buffer.queue) == 0)


# TestBufferedSubject().test_request_infinite()