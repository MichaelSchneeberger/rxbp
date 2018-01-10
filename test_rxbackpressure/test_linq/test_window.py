from unittest import TestCase

from rx.testing import TestScheduler, ReactiveTest
from rx.testing.recorded import Recorded

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.testing.backpressuremockobserver import BackpressureMockObserver
from rxbackpressure.testing.backpressuremocksubject import BackpressureMockSubject
from rxbackpressure.testing.bphotobservable import BPHotObservable
from rxbackpressure.testing.notification import bp_response

on_next = ReactiveTest.on_next
on_completed = ReactiveTest.on_completed
on_error = ReactiveTest.on_error
subscribe = ReactiveTest.subscribe
subscribed = ReactiveTest.subscribed
disposed = ReactiveTest.disposed
created = ReactiveTest.created


class TestWindow(TestCase):
    def test_window_1(self):
        subscription = [None]
        s = [None]
        scheduler = TestScheduler()

        messages1 = [
            on_next(230, (1, 2)),
        ]
        xs1 = BPHotObservable(scheduler, messages1)

        messages2 = [
            on_next(260, 1.2),
            on_next(310, 1.6),
            on_next(330, 1.8),
        ]
        xs2 = BPHotObservable(scheduler, messages2)

        parent_results = BackpressureMockSubject(
            scheduler,
            [Recorded(270, 1),
            ],
            selector=lambda v: v[0]
        )

        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(290, 1),
             Recorded(300, 1),
             Recorded(330, 1),
            ]
        )

        def action1(scheduler, state=None):
            s[0] = xs1.window(xs2, lambda v1, v2: v2 < v1[0], lambda v1, v2: v1[1] <= v2)
        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            parent_results.subscribe(on_next=lambda v: v[1].subscribe(results1))
            subscription[0] = s[0].subscribe(observer=parent_results)
        scheduler.schedule_absolute(200, action2)
        scheduler.start()

        parent_results.messages.assert_equal(
            on_next(270, (1, 2)),
        )

        results1.messages.assert_equal(
            on_next(290, 1.2),
            on_next(310, 1.6),
            on_next(330, 1.8),
        )

        results1.bp_messages.assert_equal(
            bp_response(290, 1),
            bp_response(310, 1),
            bp_response(330, 1),
        )

    def test_window_2(self):
        subscription = [None]
        s = [None]
        scheduler = TestScheduler()

        messages1 = [
            on_next(230, (1, 2)),
            on_next(240, (2, 3)),
        ]
        xs1 = BPHotObservable(scheduler, messages1)

        messages2 = [
            on_next(260, 1.2),
            on_next(280, 1.6),
            on_next(290, 1.8),
        ]
        xs2 = BPHotObservable(scheduler, messages2)

        parent_results = BackpressureMockSubject(
            scheduler,
            [Recorded(240, 1),
            ],
            selector=lambda v: v[0]
        )

        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(250, 2),
             Recorded(310, 1),
            ]
        )

        def action1(scheduler, state=None):
            s[0] = xs1.window(xs2, lambda v1, v2: v2 < v1[0], lambda v1, v2: v1[1] <= v2)
        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            parent_results.subscribe(on_next=lambda v: v[1].subscribe(results1))
            subscription[0] = s[0].subscribe(observer=parent_results)
        scheduler.schedule_absolute(200, action2)
        scheduler.start()

        print(parent_results.messages)
        print(results1.messages)
        print(results1.bp_messages)

        parent_results.messages.assert_equal(
            on_next(240, (1, 2)),
        )

        results1.messages.assert_equal(
            on_next(260, 1.2),
            on_next(280, 1.6),
            on_next(310, 1.8),
        )

        results1.bp_messages.assert_equal(
            bp_response(280, 2),
            bp_response(310, 1),
        )

    def test_window_3(self):
        subscription = [None]
        s = [None]
        scheduler = TestScheduler()
        stop_request = StopRequest()

        messages1 = [
            on_next(230, (1, 2)),
            on_next(240, (2, 3)),
        ]
        xs1 = BPHotObservable(scheduler, messages1)

        messages2 = [
            on_next(260, 1.2),
            on_next(280, 1.6),
            on_next(290, 1.8),
        ]
        xs2 = BPHotObservable(scheduler, messages2)

        parent_results = BackpressureMockSubject(
            scheduler,
            [Recorded(240, 1),
            ],
            selector=lambda v: v[0]
        )

        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(250, 2),
             Recorded(310, stop_request),
            ]
        )

        def action1(scheduler, state=None):
            s[0] = xs1.window(xs2, lambda v1, v2: v2 < v1[0], lambda v1, v2: v1[1] <= v2)
        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            parent_results.subscribe(on_next=lambda v: v[1].subscribe(results1))
            subscription[0] = s[0].subscribe(observer=parent_results)
        scheduler.schedule_absolute(200, action2)
        scheduler.start()

        print(parent_results.messages)
        print(results1.messages)
        print(results1.bp_messages)

        parent_results.messages.assert_equal(
            on_next(240, (1, 2)),
        )

        results1.messages.assert_equal(
            on_next(260, 1.2),
            on_next(280, 1.6)
        )

        results1.bp_messages.assert_equal(
            bp_response(280, 2),
            bp_response(310, stop_request),
        )

    # def test_window_4(self):
    #     subscription = [None]
    #     s = [None]
    #     scheduler = TestScheduler()
    #
    #     messages1 = [
    #         on_next(230, (1, 2)),
    #     ]
    #     xs1 = BPHotObservable(scheduler, messages1)
    #
    #     messages2 = [
    #         on_next(260, 1.2),
    #         on_next(310, 1.6),
    #         on_next(330, 1.8),
    #     ]
    #     xs2 = BPHotObservable(scheduler, messages2)
    #
    #     parent_results = BackpressureMockSubject(
    #         scheduler,
    #         [Recorded(270, 1),
    #         ],
    #         selector=lambda v: v[0]
    #     )
    #
    #     results1 = BackpressureMockObserver(
    #         scheduler,
    #         [Recorded(290, 1),
    #          Recorded(300, 1),
    #          Recorded(330, 1),
    #         ]
    #     )
    #
    #     def action1(scheduler, state=None):
    #         s[0] = xs1.window(xs2, lambda v1, v2: v2 < v1[0], lambda v1, v2: v1[1] <= v2)
    #     scheduler.schedule_absolute(100, action1)
    #
    #     def action2(scheduler, state=None):
    #         parent_results.subscribe(on_next=lambda v: v[1].subscribe(results1))
    #         subscription[0] = s[0].subscribe(observer=parent_results)
    #     scheduler.schedule_absolute(200, action2)
    #     scheduler.start()
    #
    #     parent_results.messages.assert_equal(
    #         on_next(270, (1, 2)),
    #     )
    #
    #     results1.messages.assert_equal(
    #         on_next(290, 1.2),
    #         on_next(310, 1.6),
    #         on_next(330, 1.8),
    #     )
    #
    #     results1.bp_messages.assert_equal(
    #         bp_response(290, 1),
    #         bp_response(310, 1),
    #         bp_response(330, 1),
    #     )