from rxbp.flowables.controlledzipflowable import ControlledZipFlowable
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.observesubscription import ObserveSubscription
from rxbp.subscriber import Subscriber
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestControlledZipFlowable(TestCaseBase):
    """
    """

    def setUp(self):
        self.scheduler = TestScheduler()
        self.s1 = TestFlowable(base=1)
        self.s2 = TestFlowable(base=2)
        self.sink = TestObserver()

    def test_use_case_1(self):
        flowable = ControlledZipFlowable(
            left=self.s1,
            right=self.s2,
            request_left=lambda l, r: True,
            request_right=lambda l, r: True,
            match_func=lambda l, r: True,
        )

        obs, bases = flowable.unsafe_subscribe(Subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        ))

        assert 1 in bases
        assert 2 in bases