import unittest

from rxbp.ack.ackimpl import Continue
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.observables.mergeselectorobservable import MergeSelectorObservable
from rxbp.selectors.selectionmsg import select_next, select_completed
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestMergeSelectorObservable(unittest.TestCase):
    def setUp(self) -> None:
        self.left = TestObservable()
        self.right = TestObservable()
        self.scheduler = TestScheduler()

    def test_initialize(self):
        MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )

    def test_left_select_complete_should_not_wait_on_right(self):
        """
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))

        ack = self.left.on_next_list([select_completed])

        self.assertIsInstance(ack, Continue)
        self.assertEqual([select_completed], sink.received)

    def test_wait_on_left_right_to_wait_on_right(self):
        """
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))

        ack = self.left.on_next_list([select_next, select_completed])

        self.assertFalse(ack.is_sync)
        self.assertEqual([], sink.received)

    def test_wait_on_right_to_wait_on_left_right(self):
        """
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))
        ack = self.left.on_next_list([select_next, select_completed])

        self.right.on_next_list([select_completed])

        self.assertEqual([select_completed], sink.received)

    def test_drain_left_select_completed(self):
        """
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))
        ack = self.left.on_next_list([select_next, select_completed, select_completed])

        self.right.on_next_list([select_completed])

        self.assertEqual([select_completed, select_completed], sink.received)
