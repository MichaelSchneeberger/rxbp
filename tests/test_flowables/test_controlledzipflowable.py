from rxbp.flowables.controlledzipflowable import ControlledZipFlowable
from rxbp.selectors.bases.numericalbase import NumericalBase
from rxbp.subscriber import Subscriber
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestControlledZipFlowable(TestCaseBase):
    """
    """

    def setUp(self):
        self.scheduler = TestScheduler()
        self.sink = TestObserver()

    def test_selector_dictionary(self):
        b1 = NumericalBase(1)
        b2 = NumericalBase(2)
        b3 = NumericalBase(3)
        b4 = NumericalBase(4)
        b5 = NumericalBase(5)
        s1 = TestFlowable(base=b1, selectors={b3: None, b5: None})
        s2 = TestFlowable(base=b2, selectors={b4: None})

        flowable = ControlledZipFlowable(
            left=s1,
            right=s2,
            request_left=lambda l, r: True,
            request_right=lambda l, r: True,
            match_func=lambda l, r: True,
        )

        subscription = flowable.unsafe_subscribe(Subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        ))

        assert b1 in subscription.info.selectors
        assert b2 in subscription.info.selectors
        assert b3 in subscription.info.selectors
        assert b4 in subscription.info.selectors
        assert b5 in subscription.info.selectors