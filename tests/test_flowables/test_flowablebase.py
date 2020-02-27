import unittest
from dataclasses import dataclass

from rxbp.flowablebase import FlowableBase
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testscheduler import TestScheduler


class TestFlowableBase(unittest.TestCase):
    def setUp(self):
        self.scheduler = TestScheduler()
        source = TestObservable()
        self.source = source

        @dataclass
        class TestFlowableBase(FlowableBase):
            subscriber = None

            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
                return Subscription(
                    info=BaseAndSelectors(base=None),
                    observable=source,
                )

        self.flowable = TestFlowableBase()
        self.exception = Exception('dummy')

    def test_on_next_and_on_completed_handler(self):
        on_next_buffer = []
        is_on_completed = []

        self.flowable.subscribe(
            on_next=lambda v: on_next_buffer.append(v),
            on_completed=lambda: is_on_completed.append(True),
        )

        self.source.on_next_single(1)
        self.source.on_completed()

        self.assertEqual([1], on_next_buffer)
        self.assertEqual([True], is_on_completed)

    def test_exception_handler(self):
        on_next_buffer = []
        exception_buffer = []

        self.flowable.subscribe(
            on_next=lambda v: on_next_buffer.append(v),
            on_error=lambda e: exception_buffer.append(e)
        )

        self.source.on_next_single(1)
        self.source.on_error(self.exception)

        self.assertEqual([1], on_next_buffer)
        self.assertEqual([self.exception], exception_buffer)

    def test_on_next_raises_exception(self):
        on_next_buffer = []
        exception_buffer = []

        self.flowable.subscribe(
            on_next=lambda v: on_next_buffer.append(v),
            on_error=lambda e: exception_buffer.append(e)
        )

        def gen_sequence():
            yield 1
            raise self.exception

        self.source.on_next_iter(gen_sequence())

        self.assertEqual([1], on_next_buffer)
        self.assertEqual([self.exception], exception_buffer)

    def test_default_exception_handler(self):
        self.flowable.subscribe()

        self.source.on_error(self.exception)

        # todo: how to test console output?
