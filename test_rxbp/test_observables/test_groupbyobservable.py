from typing import Callable, Any

from rx.core.typing import Disposable
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserversubscribeinner import TestObserverSubscribeInner
from rxbp.testing.testscheduler import TestScheduler
from rxbp.typing import ValueType


class GroupByObservable(Observable):
    def __init__(self, source: Observable, func: Callable[[ValueType], Any]):
        pass

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        pass


class TestControlledZipObservable(TestCaseBase):
    """
    """

    def setUp(self):
        self.scheduler = TestScheduler()
        self.source = TestObservable()
        self.sink = TestObserverSubscribeInner(inner_selector=lambda v: v, scheduler=self.scheduler)

    def test_empty_observable(self):
        obs = GroupByObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(self.sink))

        self.source.on_completed()

    def test_single_element(self):
        obs = GroupByObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(self.sink))

        ack1 = self.source.on_next_iter([1])
        self.assertListEqual(self.sink.inner_obs[0].received, [1])

    def test_single_group(self):
        obs = GroupByObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(self.sink))

        ack1 = self.source.on_next_iter([1, 1, 1])
        self.assertListEqual(self.sink.inner_obs[0].received, [1, 1, 1])

    def test1(self):
        obs = GroupByObservable(source=self.source, func=lambda v: v)
        obs.observe(ObserverInfo(self.sink))

        ack1 = self.source.on_next_iter([1, 1, 2, 1])
        self.assertListEqual(self.sink.inner_obs[0].received, [1, 1, 1])
        self.assertListEqual(self.sink.inner_obs[1].received, [2])
