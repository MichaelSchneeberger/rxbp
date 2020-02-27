from rxbp.flowables.zip2flowable import Zip2Flowable
from rxbp.selectors.bases.numericalbase import NumericalBase
from rxbp.subscriber import Subscriber
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testflowable import TestFlowable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestZipFlowable(TestCaseBase):
    """
    """

    def setUp(self):
        self.scheduler = TestScheduler()
        self.sink = TestObserver()

    def test_non_matching_base(self):
        """ A non-matching base should not create any selectors when
        gettings zipped.
        """

        b1 = NumericalBase(1)
        b2 = NumericalBase(2)
        b3 = NumericalBase(3)
        b4 = NumericalBase(4)
        s1 = TestFlowable(base=b1, selectors={b3: None})
        s2 = TestFlowable(base=b2, selectors={b4: None})

        flowable = Zip2Flowable(
            left=s1,
            right=s2,
        )

        subscription = flowable.unsafe_subscribe(Subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        ))

        self.assertIsNone(subscription.info.selectors)

    def test_matching_base(self):
        b1 = NumericalBase(1)
        b2 = NumericalBase(1)
        b3 = NumericalBase(3)
        b4 = NumericalBase(4)
        b5 = NumericalBase(5)
        s1 = TestFlowable(base=b1, selectors={b3: None})
        s2 = TestFlowable(base=b2, selectors={b4: None, b5: None})

        flowable = Zip2Flowable(
            left=s1,
            right=s2,
        )

        subscription = flowable.unsafe_subscribe(Subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        ))

        self.assertIn(b3, subscription.info.selectors)
        self.assertIn(b4, subscription.info.selectors)
        self.assertIn(b5, subscription.info.selectors)
