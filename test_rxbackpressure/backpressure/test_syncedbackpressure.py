from unittest import TestCase

import math

from rx.core.notification import OnNext, OnCompleted
from rx.testing import TestScheduler, ReactiveTest
from rx.testing.recorded import Recorded

from rxbackpressure.backpressuretypes.bufferbackpressure import BufferBackpressure
from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.backpressuretypes.syncedbackpressure import SyncedBackpressure
from rxbackpressure.buffers.dequeuablebuffer import DequeuableBuffer
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.subjects.controlledsubject import ControlledSubject
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



class TestSyncedBackpressure(TestCase):
    def test1(self):
        scheduler = TestScheduler()

        stop_request = StopRequest()

        observable = BPHotObservable(scheduler=scheduler,
                                     messages=[on_next(130, 1),
                                               on_next(250, 2),
                                               ])
        observable.subscribe(BackpressureMockObserver(scheduler, []))

        observer = BackpressureMockObserver(
            scheduler,
            [Recorded(110, 1),
             Recorded(210, 1),
             Recorded(230, stop_request),
             ]
        )

        backpressure = SyncedBackpressure(scheduler=scheduler)
        backpressure.add_backpressure(observable.backpressure)
        backpressure.add_observer(observer)

        scheduler.start()

        print(observer.messages)
        print(observer.bp_messages)

        observer.bp_messages.assert_equal(
            bp_response(130, 1),
            bp_response(250, 1),
            bp_response(250, stop_request),
        )

    def test2(self):
        scheduler = TestScheduler()

        stop_request = StopRequest()

        observable = BPHotObservable(scheduler=scheduler,
                                     messages=[on_next(130, 1),
                                               on_next(250, 2),
                                               on_next(260, 3),
                                               on_next(270, 4),
                                               on_next(290, 5),
                                               ])
        observable.subscribe(BackpressureMockObserver(scheduler, []))

        observer1 = BackpressureMockObserver(
            scheduler,
            [Recorded(110, 1),
             Recorded(210, 1),
             Recorded(230, stop_request),
             ]
        )

        observer2 = BackpressureMockObserver(
            scheduler,
            [Recorded(140, 1),
             Recorded(200, 1),
             Recorded(220, 1),
             Recorded(280, 1),
             Recorded(300, stop_request),
             ]
        )

        backpressure = SyncedBackpressure(scheduler=scheduler)
        backpressure.add_backpressure(observable.backpressure)
        backpressure.add_observer(observer1)
        backpressure.add_observer(observer2)

        scheduler.start()

        observer1.bp_messages.assert_equal(
            bp_response(141, 1),
            bp_response(250, 1),
            bp_response(250, stop_request),
        )

        observer2.bp_messages.assert_equal(
            bp_response(141, 1),
            bp_response(250, 1),
            bp_response(260, 1),
            bp_response(281, 1),
            bp_response(301, stop_request),
        )

TestSyncedBackpressure().test2()
