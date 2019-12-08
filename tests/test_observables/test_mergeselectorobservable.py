import unittest

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

    def test_simple(self):
        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))

        self.left.on_next_list([select_next, select_completed])
        self.right.on_next_list([select_completed])

        print(sink.received)
