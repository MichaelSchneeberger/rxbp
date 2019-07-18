from rxbp.ack.ack import Ack
from rxbp.ack.ackimpl import continue_ack
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.observesubscription import ObserveSubscription
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestConnectableSubscriber(TestCaseBase):
    """
    Zip2Observable is a stateful object, therefore we test methods of Zip2Observable as a function of its states
    called "zip_state" of type Zip2Observable.ZipState and "termination_state" of type Zip2Observable.TerminationState.
    The termination state has four data types, which possibly have their own states. The zip state has five data
    types, which possibly have their own states as well.

    Zip2Observable is symmetric to left and right source Observable. Therefore, the test specific to the left source
    Observable can be mirrored to get the test for the right source Observable.

    For non-concurrent testing, the state ZipElements can be ignored.

    Restricted method calls according to rxbackpressure conventions:
    1. left.on_next and right.on_next in state=ZipElements
    2. left.on_next in state=WaitForRight
    3. right.on_next in state=WaitForLeft

    """

    def setUp(self):
        self.scheduler = TestScheduler()
        self.s1 = TestObservable()
        self.s2 = TestObservable()
        self.sink = TestObserver()

    def test_init_termination_state_wait_on_left_right_immediate_ack(self):
        obs = Zip2Observable(self.s1, self.s2)
        obs.observe(ObserveSubscription(self.sink))

        self.sink.immediate_continue = 10

        self.assertIsInstance(obs.zip_state.get_current_state(obs.termination_state), Zip2Observable.WaitOnLeftRight)

        # state WaitOnLeftRight -> WaitOnRight
        ack1 = self.s1.on_next_seq([1, 2, 3, 4])
        self.assertListEqual(self.sink.received, [])

        self.assertIsInstance(obs.zip_state.get_current_state(obs.termination_state), Zip2Observable.WaitOnRight)

        # state WaitOnRight -> WaitOnRight
        ack2 = self.s2.on_next_seq([11, 12])
        self.assertListEqual(self.sink.received, [(1, 11), (2, 12)])
        self.assertIsInstance(obs.zip_state.get_current_state(obs.termination_state), Zip2Observable.WaitOnRight)

        self.assertFalse(ack1.has_value)
        self.assertTrue(ack2.has_value)

        # state WaitOnRight -> WaitOnLeftRight
        self.s2.on_next_seq([13, 14])
        self.assertListEqual(self.sink.received, [(1, 11), (2, 12), (3, 13), (4, 14)])
        self.assertIsInstance(obs.zip_state.get_current_state(obs.termination_state), Zip2Observable.WaitOnLeftRight)

        self.s1.on_completed()
        self.assertTrue(self.sink.is_completed)

        self.s2.on_next_seq([13, 14])
        self.assertListEqual(self.sink.received, [(1, 11), (2, 12), (3, 13), (4, 14)])
        self.assertIsInstance(obs.zip_state.get_current_state(obs.termination_state), Zip2Observable.Stopped)

    def test_init_termination_state_wait_on_left_right_delayed_ack(self):
        obs = Zip2Observable(self.s1, self.s2)
        obs.observe(ObserveSubscription(self.sink))

        # state WaitOnLeftRight -> WaitOnRight
        ack1: Ack = self.s1.on_next_seq([1, 2, 3, 4])
        self.assertListEqual(self.sink.received, [])
        self.assertIsInstance(obs.zip_state.get_current_state(obs.termination_state), Zip2Observable.WaitOnRight)

        # state WaitOnRight -> WaitOnRight
        ack2 = self.s2.on_next_seq([11, 12])
        self.assertListEqual(self.sink.received, [(1, 11), (2, 12)])
        self.assertIsInstance(obs.zip_state.get_current_state(obs.termination_state), Zip2Observable.WaitOnRight)
        self.assertFalse(ack1.has_value)
        self.assertFalse(ack2.has_value)

        self.sink.ack.on_next(continue_ack)
        # back-pressure right
        self.assertFalse(ack1.has_value)
        self.assertTrue(ack2.has_value)

        # state WaitOnRight -> WaitOnLeftRight
        ack2 = self.s2.on_next_seq([13, 14])
        self.assertListEqual(self.sink.received, [(1, 11), (2, 12), (3, 13), (4, 14)])
        self.assertIsInstance(obs.zip_state.get_current_state(obs.termination_state), Zip2Observable.WaitOnLeftRight)

        self.sink.ack.on_next(continue_ack)
        # back-pressure both
        self.assertTrue(ack1.has_value)
        self.assertTrue(ack2.has_value)

    def test_init_termination_state_wait_on_right_immediate_ack(self):
        obs = Zip2Observable(self.s1, self.s2)
        obs.observe(ObserveSubscription(self.sink))

        self.sink.immediate_continue = 10

        # state WaitOnLeftRight -> WaitOnRight
        ack1 = self.s1.on_next_seq([1, 2])
        self.assertListEqual(self.sink.received, [])
        self.assertIsInstance(obs.zip_state.get_current_state(obs.termination_state), Zip2Observable.WaitOnRight)

        # state WaitOnRight -> WaitOnLeft
        ack2 = self.s2.on_next_seq([11, 12, 13, 14])
        self.assertListEqual(self.sink.received, [(1, 11), (2, 12)])
        self.assertIsInstance(obs.zip_state.get_current_state(obs.termination_state), Zip2Observable.WaitOnLeft)

        self.assertTrue(ack1.has_value)
        self.assertFalse(ack2.has_value)

        self.s1.on_completed()
        self.assertTrue(self.sink.is_completed)
