from rxbp.ack.ackimpl import continue_ack, Continue
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.observerinfo import ObserverInfo
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
        # sink = TestObserver()

    class Command:
        pass

    class Go(Command):
        pass

    class Stop(Command):
        pass

    def test_use_case_1_sync_ack(self):
        sink = TestObserver()
        obs = ControlledZipObservable(left=self.s1, right=self.s2, scheduler=self.scheduler,
                                      request_left=lambda l, r: True,
                                      request_right=lambda l, r: isinstance(l, self.Go),
                                      match_func=lambda l, r: True)
        obs.observe(ObserverInfo(sink))

        go = self.Go()
        stop = self.Stop()

        ack1 = self.s1.on_next_iter([go, stop, stop, go])
        self.assertListEqual(sink.received, [])

        ack2 = self.s2.on_next_iter([1, 2, 3])
        self.assertListEqual(sink.received, [(go, 1), (stop, 2), (stop, 2), (go, 2)])

    def test_use_case_1_async_ack(self):
        sink = TestObserver(immediate_continue=0)
        obs = ControlledZipObservable(left=self.s1, right=self.s2, scheduler=self.scheduler,
                                      request_left=lambda l, r: True,
                                      request_right=lambda l, r: isinstance(l, self.Go),
                                      match_func=lambda l, r: True)
        obs.observe(ObserverInfo(sink))

        go = self.Go()
        stop = self.Stop()

        ack1 = self.s1.on_next_iter([go, stop])
        self.assertListEqual(sink.received, [])

        ack2 = self.s2.on_next_iter([1, 2, 3])
        self.assertListEqual(sink.received, [(go, 1), (stop, 2)])

        sink.ack.on_next(continue_ack)
        self.assertTrue(ack1.has_value)

        ack3 = self.s1.on_next_iter([stop, go])
        self.assertListEqual(sink.received, [(go, 1), (stop, 2), (stop, 2), (go, 2)])

        sink.ack.on_next(continue_ack)
        self.assertTrue(ack3.has_value)
