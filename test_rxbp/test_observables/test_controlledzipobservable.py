from rxbp.ack.ackimpl import continue_ack, Continue
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.observesubscription import ObserveSubscription
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestControlledZipObservable(TestCaseBase):
    """
    """

    def setUp(self):
        self.scheduler = TestScheduler()
        self.s1 = TestObservable()
        self.s2 = TestObservable()
        self.sink = TestObserver()

    class Command:
        pass

    class Go(Command):
        pass

    class Stop(Command):
        pass

    def test_use_case_1_sync_ack(self):

        obs = ControlledZipObservable(left=self.s1, right=self.s2, scheduler=self.scheduler,
                                      request_left=lambda l, r: True,
                                      request_right=lambda l, r: isinstance(l, self.Go),
                                      match_func=lambda l, r: True)
        obs.observe(ObserveSubscription(self.sink))

        go = self.Go()
        stop = self.Stop()

        self.sink.immediate_continue = 10

        ack1 = self.s1.on_next_seq([go, stop, stop, go])
        self.assertListEqual(self.sink.received, [])

        ack2 = self.s2.on_next_seq([1, 2, 3])
        self.assertListEqual(self.sink.received, [(go, 1), (stop, 2), (stop, 2), (go, 2)])

    def test_use_case_1_async_ack(self):

        obs = ControlledZipObservable(left=self.s1, right=self.s2, scheduler=self.scheduler,
                                      request_left=lambda l, r: True,
                                      request_right=lambda l, r: isinstance(l, self.Go),
                                      match_func=lambda l, r: True)
        obs.observe(ObserveSubscription(self.sink))

        go = self.Go()
        stop = self.Stop()

        ack1 = self.s1.on_next_seq([go, stop])
        self.assertListEqual(self.sink.received, [])

        ack2 = self.s2.on_next_seq([1, 2, 3])
        self.assertListEqual(self.sink.received, [(go, 1), (stop, 2)])

        self.sink.ack.on_next(continue_ack)
        self.assertTrue(ack1.has_value)

        ack3 = self.s1.on_next_seq([stop, go])
        self.assertListEqual(self.sink.received, [(go, 1), (stop, 2), (stop, 2), (go, 2)])

        self.sink.ack.on_next(continue_ack)
        self.assertTrue(ack3.has_value)
