from unittest import TestCase

from rx.testing import TestScheduler, ReactiveTest
from rx.testing.recorded import Recorded

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.subjects.syncedsubject import SyncedSubject
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


class TestSyncedBPSubject(TestCase):

    def test_synced_subject_1(self):
        subscription = [None]
        s = [None]
        subject = SyncedSubject()
        scheduler = TestScheduler()

        messages1 = [
            on_next(230, 1),
            on_next(260, 2),
            on_next(270, 3),
            on_next(280, 4),
            on_next(290, 5),
            on_next(320, 6),
        ]
        xs1 = BPHotObservable(scheduler, messages1)


        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(250, 2),
             Recorded(300, 2),
             Recorded(310, 1),
            ]
        )

        def action1(scheduler, state=None):
            subject.subscribe(results1, scheduler=scheduler)
        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            s[0] = xs1.subscribe(subject, scheduler=scheduler)
        scheduler.schedule_absolute(200, action2)

        scheduler.start()

        print(results1.messages)
        print(results1.bp_messages)

        results1.messages.assert_equal(
            on_next(250, 1),
            on_next(260, 2),
            on_next(300, 3),
            on_next(300, 4),
            on_next(310, 5),
        )

        results1.bp_messages.assert_equal(
            bp_response(260, 2),
            bp_response(300, 2),
            bp_response(310, 1),
        )

    def test_synced_subject_2(self):
        subscription = [None]
        s = [None]
        subject = SyncedSubject()
        scheduler = TestScheduler()

        stop_request = StopRequest()

        messages1 = [
            on_next(230, 1),
            on_next(260, 2),
            on_next(290, 3),
        ]
        xs1 = BPHotObservable(scheduler, messages1)


        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(250, 1),
             Recorded(280, stop_request)
            ]
        )

        def action1(scheduler, state=None):
            subject.subscribe(results1)

        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            s[0] = xs1.subscribe(subject)

        scheduler.schedule_absolute(200, action2)
        scheduler.start()

        print(results1.messages)
        print(results1.bp_messages)

        results1.messages.assert_equal(
            on_next(250, 1),
            on_completed(280)
        )

        results1.bp_messages.assert_equal(
            bp_response(250, 1),
            bp_response(280, stop_request)
        )

    def test_synced_subject_2(self):
        subscription = [None]
        s = [None]
        subject = SyncedSubject()
        scheduler = TestScheduler()

        messages1 = [
            on_next(230, 1),
            on_next(240, 2),
            on_next(290, 3),
            on_next(300, 4),
            on_completed(330)
        ]
        xs1 = BPHotObservable(scheduler, messages1)


        results1 = BackpressureMockObserver(
            scheduler,
            [Recorded(210, 1),
             Recorded(220, 1),
             Recorded(230, 1),
             Recorded(320, 1),
             Recorded(350, 1)
            ]
        )

        results2 = BackpressureMockObserver(
            scheduler,
            [Recorded(250, 1),
             Recorded(260, 1),
             Recorded(280, 1),
             Recorded(310, 1),
             Recorded(340, 1)
            ]
        )

        def action1(scheduler, state=None):
            subject.subscribe(results1)
            subject.subscribe(results2)
        scheduler.schedule_absolute(100, action1)

        def action2(scheduler, state=None):
            s[0] = xs1.subscribe(subject)
        scheduler.schedule_absolute(200, action2)

        scheduler.start()

        print(results1.messages)
        print(results1.bp_messages)

        results1.messages.assert_equal(
            on_next(250, 1),
            on_next(260, 2),
            on_next(290, 3),
            on_next(320, 4),
            on_completed(350)
        )

        results1.bp_messages.assert_equal(
            bp_response(250, 1),
            bp_response(260, 1),
            bp_response(290, 1),
            bp_response(320, 1),
            bp_response(350, 0),
        )
