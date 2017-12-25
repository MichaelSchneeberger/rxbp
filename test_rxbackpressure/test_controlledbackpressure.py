from unittest import TestCase

import math

from rx.core.notification import OnNext
from rx.testing import TestScheduler, ReactiveTest
from rx.testing.recorded import Recorded

from rxbackpressure.backpressuretypes.controlledbackpressure import ControlledBackpressure
from rxbackpressure.buffers.dequeuablebuffer import DequeuableBuffer
from rxbackpressure.subjects.controlledsubject import ControlledSubject
from rxbackpressure.testing.backpressuremockobserver import BackpressureMockObserver
from rxbackpressure.testing.notification import bp_response

on_next = ReactiveTest.on_next
on_completed = ReactiveTest.on_completed
on_error = ReactiveTest.on_error
subscribe = ReactiveTest.subscribe
subscribed = ReactiveTest.subscribed
disposed = ReactiveTest.disposed
created = ReactiveTest.created



class TestControlledBackpressure(TestCase):
    def test1(self):
        scheduler = TestScheduler()

        def update(v):
            # print('append {}'.format(v))
            buffer.append(OnNext(v))
            backpressure.update()

        buffer = DequeuableBuffer()
        scheduler.create_hot_observable(
            on_next(120, 1),
            on_next(210, 2),
            on_next(240, 3)
        ).subscribe(update)

        observer = BackpressureMockObserver(
            scheduler,
            [Recorded(110, 1),
             Recorded(230, 2),
             ]
        )

        def update_source(proxy, idx):
            pass

        def dispose(proxy):
            pass

        backpressure = ControlledBackpressure(buffer=buffer,
                                              last_idx=0,
                                              observer=observer,
                                              update_source=update_source,
                                              dispose=dispose)

        scheduler.start()

        observer.messages.assert_equal(
            on_next(120, 1),
            on_next(230, 2),
            on_next(240, 3)
        )

        observer.bp_messages.assert_equal(
            bp_response(120, 1),
            bp_response(240, 2),
        )

TestControlledBackpressure().test1()
