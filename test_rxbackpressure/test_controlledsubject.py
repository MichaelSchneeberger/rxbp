from unittest import TestCase

import math
from rx.testing import TestScheduler, ReactiveTest
from rx.testing.recorded import Recorded

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


class TestControlledSubject(TestCase):
    def test_request_one_element_in_infinit_sequence(self):
        subscription = [None]
        s = [None]
        scheduler = TestScheduler()

        xs = scheduler.create_hot_observable(
            on_next(70, 1),
            on_next(110, 2),
            on_next(220, 3),
            on_next(340, 4),
            on_next(420, 5),
            on_next(520, 6),
            on_next(630, 7),
        )

        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(380, 1),
             Recorded(390, 1),
             Recorded(400, 1),
             Recorded(410, 1),
             ]
        )

        # create
        def action1(scheduler, state=None):
            s[0] = BufferedSubject()
        scheduler.schedule_absolute(100, action1)

        # subscribe
        def action2(scheduler, state=None):
            xs.subscribe(s[0])
        scheduler.schedule_absolute(200, action2)

        def action4(scheduler, state=None):
            subscription[0] = s[0].subscribe(observer=results1, subscribe_bp=results1.subscribe_backpressure)
        scheduler.schedule_absolute(300, action4)

        # dispose
        def action3(scheduler, state=None):
            subscription[0].dispose()
        scheduler.schedule_absolute(600, action3)

        scheduler.start()

        results1.messages.assert_equal(
            on_next(380, 4),
            on_next(420, 5),
            on_next(520, 6),
        )

    def test_request_one_element_in_finit_sequence(self):
        subscription = [None]
        s = [None]
        scheduler = TestScheduler()

        xs = scheduler.create_hot_observable(
            on_next(70, 1),
            on_next(110, 2),
            on_next(220, 3),
            on_next(340, 4),
            on_next(420, 5),
            on_completed(510),
        )

        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(250, 1),
             Recorded(380, 1),
             Recorded(400, 1),
             Recorded(530, 1),
             Recorded(580, 1)]
        )

        def action1(scheduler, state=None):
            s[0] = BufferedSubject()
        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            subscription[0] = xs.subscribe(s[0])
        scheduler.schedule_absolute(200, action2)

        def action4(scheduler, state=None):
            subscription[0] = s[0].subscribe(observer=results1, subscribe_bp=results1.subscribe_backpressure)
        scheduler.schedule_absolute(300, action4)
        scheduler.start()

        def action3(scheduler, state=None):
            subscription[0].dispose()
        scheduler.schedule_absolute(600, action3)

        print(results1.messages)
        print(results1.bp_messages)

        results1.messages.assert_equal(
            on_next(380, 4),
            on_next(420, 5),
            on_completed(530)
        )

        results1.bp_messages.assert_equal(
            bp_response(380, 1),
            bp_response(420, 1),
            bp_response(530, 0),
            bp_response(580, 0),
        )
    #
    #
    # def test_request_one_element_and_disposing_sequence(self):
    #     subscription = [None]
    #     s = [None]
    #     scheduler = TestScheduler()
    #
    #     xs = scheduler.create_hot_observable(
    #         on_next(70, 1),
    #         on_next(230, 2),
    #         # on_next(660, 5),
    #         # on_completed(710),
    #     )
    #
    #     results1 = BackpressureMockObserver(
    #         scheduler,
    #         [Recorded(250, 1),
    #          Recorded(380, 1),
    #          Recorded(530, 1),
    #          Recorded(610, 1)]
    #     )
    #
    #     def action1(scheduler, state=None):
    #         s[0] = ControlledSubject()
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
    #     results1.messages.assert_equal(
    #         on_next(380, 2),
    #     )
    #
    #     results1.bp_messages.assert_equal(
    #         bp_response(380, 1),
    #         bp_response(610, 0),
    #     )
    #
    #
    def test_request_multible_elements(self):
        subscription = [None]
        s = [None]
        scheduler = TestScheduler()

        xs = scheduler.create_hot_observable(
            on_next(70, 1),
            on_next(110, 2),
            on_next(220, 3),
            on_next(340, 4),
            on_next(420, 5),
            on_next(520, 6),
            on_next(630, 7),
        )

        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(320, 3),
             Recorded(330, 1)]
        )

        def action1(scheduler, state=None):
            s[0] = BufferedSubject()
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
            on_next(340, 4),
            on_next(420, 5),
            on_next(520, 6),
        )

        results1.bp_messages.assert_equal(
            bp_response(520, 3),
        )

    #
    #
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
    #         s[0] = ControlledSubject()
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
    #
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
    #          Recorded(430, math.inf)]
    #     )
    #
    #     def action1(scheduler, state=None):
    #         s[0] = ControlledSubject()
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
    #     results1.messages.assert_equal(
    #         on_next(340, 4),
    #     )
    #
    #     print([e.value for e in s[0].buffer.queue])
    #
    #     assert(len(s[0].buffer.queue) == 0)
