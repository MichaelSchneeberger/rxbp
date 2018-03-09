from unittest import TestCase

import math

from rx.core.notification import OnNext, OnCompleted
from rx.testing import TestScheduler, ReactiveTest
from rx.testing.recorded import Recorded

from rxbackpressure.backpressuretypes.bufferbackpressure import BufferBackpressure
from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.buffers.dequeuablebuffer import DequeuableBuffer
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



class TestBufferBackpressure(TestCase):
    # def test1(self):
    #     scheduler = TestScheduler()
    #
    #     def update(v):
    #         # print('append {}'.format(v))
    #         buffer.append(OnNext(v))
    #         backpressure.update()
    #
    #     def complete():
    #         buffer.append(OnCompleted())
    #         backpressure.update()
    #
    #     buffer = DequeuableBuffer()
    #     scheduler.create_hot_observable(
    #         on_next(110, 1),
    #         on_next(210, 2),
    #         on_next(220, 3),
    #         on_next(260, 4),
    #         on_next(270, 5),
    #         on_next(280, 6),
    #         on_next(290, 7),
    #         on_next(300, 8),
    #         on_completed(350)
    #     ).subscribe(update, on_completed=complete)
    #
    #     observer = BackpressureMockObserver(
    #         scheduler,
    #         [Recorded(120, 1),
    #          Recorded(230, 3),
    #          Recorded(310, 2),
    #          Recorded(320, 3),
    #          ]
    #     )
    #
    #     def update_source(proxy, idx):
    #         print(idx)
    #
    #     def dispose(proxy):
    #         pass
    #
    #     backpressure = BufferBackpressure(buffer=buffer,
    #                                       last_idx=0,
    #                                       observer=observer,
    #                                       update_source=update_source,
    #                                       dispose=dispose)
    #
    #     scheduler.start()
    #
    #     print(observer.messages)
    #     print(observer.bp_messages)
    #
    #     observer.messages.assert_equal(
    #         on_next(120, 1),
    #         on_next(230, 2),
    #         on_next(230, 3),
    #         on_next(260, 4),
    #         on_next(310, 5),
    #         on_next(310, 6),
    #         on_next(320, 7),
    #         on_next(320, 8),
    #         on_completed(350),
    #     )
    #
    #     observer.bp_messages.assert_equal(
    #         bp_response(120, 1),
    #         bp_response(260, 3),
    #         bp_response(310, 2),
    #         bp_response(350, 2),
    #     )
    #
    #     # assert(backpressure.update() is 4)

    def test2(self):
        scheduler = TestScheduler()

        def update(v):
            # print('append {}'.format(v))
            buffer.append(OnNext(v))
            backpressure.update()

        def complete():
            buffer.append(OnCompleted())
            backpressure.update()

        buffer = DequeuableBuffer()
        scheduler.create_hot_observable(
            on_next(250, 1),
            on_next(260, 2),
            on_next(270, 3),
            on_next(280, 4),
            on_next(290, 5),
            on_next(300, 6),
            on_next(310, 7),
            on_next(320, 8),
            on_completed(350)
        ).subscribe(update, on_completed=complete)

        observer = BackpressureMockObserver(
            scheduler,
            [Recorded(210, 1),
             Recorded(220, 3),
             Recorded(230, 2),
             Recorded(240, 3),
             ]
        )

        def update_source(proxy, idx):
            # print(idx)
            pass

        def dispose(proxy):
            pass

        backpressure = BufferBackpressure(buffer=buffer,
                                          last_idx=0,
                                          observer=observer,
                                          update_source=update_source,
                                          dispose=dispose)

        scheduler.start()

        print(observer.messages)
        print(observer.bp_messages)

        observer.messages.assert_equal(
            on_next(250, 1),
            on_next(260, 2),
            on_next(270, 3),
            on_next(280, 4),
            on_next(290, 5),
            on_next(300, 6),
            on_next(310, 7),
            on_next(320, 8),
            on_completed(350),
        )

        observer.bp_messages.assert_equal(
            bp_response(250, 1),
            bp_response(280, 3),
            bp_response(300, 2),
            bp_response(350, 2),
        )

        # assert(backpressure.update() is 4)

    # def test3(self):
    #     scheduler = TestScheduler()
    #
    #     def update(v):
    #         # print('append {}'.format(v))
    #         buffer.append(OnNext(v))
    #         backpressure.update()
    #
    #     buffer = DequeuableBuffer()
    #     scheduler.create_hot_observable(
    #         on_next(120, 1),
    #         on_next(210, 2),
    #         on_next(240, 3),
    #     ).subscribe(update)
    #
    #     stop_request = StopRequest()
    #     observer = BackpressureMockObserver(
    #         scheduler,
    #         [Recorded(110, 1),
    #          Recorded(220, 2),
    #          Recorded(230, stop_request),
    #          ]
    #     )
    #
    #     def update_source(proxy, idx):
    #         pass
    #
    #     def dispose(proxy):
    #         pass
    #
    #     backpressure = BufferBackpressure(buffer=buffer,
    #                                       last_idx=0,
    #                                       observer=observer,
    #                                       update_source=update_source,
    #                                       dispose=dispose)
    #
    #     scheduler.start()
    #
    #     print(observer.messages)
    #     print(observer.bp_messages)
    #
    #     observer.messages.assert_equal(
    #         on_next(120, 1),
    #         on_next(220, 2),
    #         on_next(240, 3),
    #         on_completed(240)
    #     )
    #
    #     observer.bp_messages.assert_equal(
    #         bp_response(120, 1),
    #         bp_response(240, 2),
    #         bp_response(240, stop_request),
    #     )
    #
    # assert(backpressure.update() is 3)

# TestBufferBackpressure().test2()
