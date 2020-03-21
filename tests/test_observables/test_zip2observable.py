from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.states.measuredstates.zipstates import ZipStates
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestZip2Observable(TestCaseBase):
    """
    Zip2Observable is a stateful object, therefore we test methods of Zip2Observable as a function of its states
    called "zip_state" of type Zip2Observable.ZipState and "termination_state" of type Zip2Observable.TerminationState.
    The termination state has four data types, which possibly have their own states. The join_flowables state has five data
    types, which possibly have their own states as well.

    Zip2Observable is symmetric to left and right source Observable. Therefore, the test specific to the left source
    Observable can be mirrored to get the test for the right source Observable.

    For non-concurrent testing, the state ZipElements can be ignored.

    The following method calls are prohibited by the rxbackpressure conventions:
    1. left.on_next and right.on_next in state=ZipElements
    2. left.on_next in state=WaitForRight
    3. right.on_next in state=WaitForLeft

    The drawings in the comments for each test follow the following convention:

               action
    state1 ---------------> state2

    state1: the state of ZipObservable before the action is applied
    action: on_next, on_completed, ack.on_next function calls
    state2: the state immediately after the action is applied

    """

    def setUp(self):
        self.scheduler = TestScheduler()
        self.left = TestObservable()
        self.right = TestObservable()
        self.exception = Exception('test')

    def measure_state(self, obs: Zip2Observable):
        return obs.state.get_measured_state(obs.termination_state)

    def measure_termination_state(self, obs: Zip2Observable):
        return obs.termination_state.get_measured_state()

    def test_init_state(self):
        sink = TestObserver()
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertIsInstance(self.measure_state(obs), ZipStates.WaitOnLeftRight)

    def test_left_complete(self):
        """
                         s1.on_completed
        WaitOnLeftRight -----------------> Stopped
         InitState                   LeftCompletedState
        """

        sink = TestObserver()
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        self.left.on_completed()

        self.assertIsInstance(self.measure_state(obs), ZipStates.Stopped)
        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.LeftCompletedState)
        self.assertTrue(sink.is_completed)

    def test_right_complete(self):
        """
                         s2.on_completed
        WaitOnLeftRight -----------------> Stopped
         InitState                   RightCompletedState
        """

        sink = TestObserver()
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        self.right.on_completed()

        self.assertIsInstance(self.measure_state(obs), ZipStates.Stopped)
        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.RightCompletedState)
        self.assertTrue(sink.is_completed)

    def test_emit_left_with_synchronous_ack(self):
        """
                         s1.on_next
        WaitOnLeftRight ------------> WaitOnRight
         InitState                    InitState
        """

        sink = TestObserver()
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        ack1 = self.left.on_next_single(1)

        self.assertIsInstance(self.measure_state(obs), ZipStates.WaitOnRight)
        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertFalse(ack1.has_value)
        self.assertListEqual(sink.received, [])

    def test_zip_single_element_with_synchronous_ack(self):
        """
                     s2.on_next
        WaitOnRight ------------> WaitOnLeftRight
        """

        sink = TestObserver()
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))
        ack1 = self.left.on_next_single(1)

        ack2 = self.right.on_next_single(1)

        self.assertIsInstance(self.measure_state(obs), ZipStates.WaitOnLeftRight)
        self.assertIsInstance(ack1.value, ContinueAck)
        self.assertIsInstance(ack2, ContinueAck)
        self.assertListEqual(sink.received, [(1, 1)])

    def test_multiple_elements_with_synchronous_ack(self):
        """
                     s2.on_next
        WaitOnRight ------------> WaitOnLeftRight
        """

        sink = TestObserver()
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        ack1 = self.left.on_next_list([1, 2, 3])
        ack2 = self.right.on_next_list([1, 2, 3])

        self.assertIsInstance(self.measure_state(obs), ZipStates.WaitOnLeftRight)
        self.assertIsInstance(ack1.value, ContinueAck)
        self.assertIsInstance(ack2, ContinueAck)
        self.assertListEqual(sink.received, [(1, 1), (2, 2), (3, 3)])

    def test_wait_on_right_to_wait_on_right_with_synchronous_ack(self):
        """
                     s2.on_next
        WaitOnRight ------------> WaitOnRight
        """

        sink = TestObserver()
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        ack1 = self.left.on_next_list([1, 2])
        ack2 = self.right.on_next_list([1])

        self.assertIsInstance(self.measure_state(obs), ZipStates.WaitOnRight)
        self.assertFalse(ack1.has_value)
        self.assertIsInstance(ack2, ContinueAck)
        self.assertListEqual(sink.received, [(1, 1)])

    def test_wait_on_right_to_wait_on_left_with_synchronous_ack(self):
        """
                     s2.on_next
        WaitOnRight ------------> WaitOnLeft
        """

        sink = TestObserver()
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        ack1 = self.left.on_next_list([1, 2])
        ack2 = self.right.on_next_list([1, 2, 3])

        self.assertIsInstance(self.measure_state(obs), ZipStates.WaitOnLeft)
        self.assertIsInstance(ack1.value, ContinueAck)
        self.assertFalse(ack2.has_value)
        self.assertListEqual(sink.received, [(1, 1), (2, 2)])

    def test_acknowledge_both(self):
        """
                        ack.on_next
        WaitOnRightLeft ------------> WaitOnRightLeft
        """

        sink = TestObserver(immediate_continue=0)
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        ack1 = self.left.on_next_list([1])
        ack2 = self.right.on_next_list([1])
        sink.ack.on_next(continue_ack)

        self.assertIsInstance(self.measure_state(obs), ZipStates.WaitOnLeftRight)
        self.assertIsInstance(ack1.value, ContinueAck)
        self.assertIsInstance(ack2.value, ContinueAck)
        self.assertListEqual(sink.received, [(1, 1)])

    def test_acknowledge_left(self):
        """
                    ack.on_next
        WaitOnLeft ------------> WaitOnLeft
        """

        sink = TestObserver(immediate_continue=0)
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        ack1 = self.left.on_next_list([1])
        ack2 = self.right.on_next_list([1, 2])
        sink.ack.on_next(continue_ack)

        self.assertIsInstance(self.measure_state(obs), ZipStates.WaitOnLeft)
        self.assertIsInstance(ack1.value, ContinueAck)
        self.assertFalse(ack2.has_value)
        self.assertListEqual(sink.received, [(1, 1)])

    def test_acknowledge_after_completed(self):
        """
                ack.on_next
        Stopped ------------> Stopped
        """

        sink = TestObserver(immediate_continue=0)
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        ack1 = self.left.on_next_list([1])
        ack2 = self.right.on_next_list([1])
        self.left.on_completed()
        sink.ack.on_next(continue_ack)

        self.assertIsInstance(self.measure_state(obs), ZipStates.Stopped)
        self.assertListEqual(sink.received, [(1, 1)])

    def test_exception(self):
        """
                    ack.on_next
        WaitOnRight ------------> Stopped
        """

        sink = TestObserver(immediate_continue=0)
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        ack1 = self.left.on_next_list([1])
        self.right.on_error(self.exception)

        self.assertIsInstance(self.measure_state(obs), ZipStates.Stopped)
        self.assertEqual(sink.exception, self.exception)

    def test_left_complete_wait_on_right(self):
        """
                        s1.on_completed
        WaitOnLeftRight ------------> WaitOnRight
          InitState                LeftCompletedState
        """

        sink = TestObserver(immediate_continue=0)
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        self.left.on_next_list([1])
        self.left.on_completed()

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.LeftCompletedState)
        self.assertIsInstance(self.measure_state(obs), ZipStates.WaitOnRight)

    def test_left_complete_to_stopped(self):
        """
                    s2.on_next
        WaitOnRight ------------> Stopped
        LeftComplete              BothCompletedState
        """

        sink = TestObserver(immediate_continue=0)
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        self.left.on_next_list([1])
        self.left.on_completed()
        self.right.on_next_list([1])

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.LeftCompletedState)
        self.assertIsInstance(self.measure_state(obs), ZipStates.Stopped)
        self.assertListEqual(sink.received, [(1, 1)])

    def test_left_complete_to_wait_on_right(self):
        """
                    s2.on_next
        WaitOnRight ------------> WaitOnRight
        LeftComplete              LeftComplete
        """

        sink = TestObserver(immediate_continue=0)
        obs = Zip2Observable(self.left, self.right)
        obs.observe(ObserverInfo(sink))

        self.left.on_next_list([1, 1])
        self.left.on_completed()
        self.right.on_next_list([1])

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.LeftCompletedState)
        self.assertIsInstance(self.measure_state(obs), ZipStates.WaitOnRight)
        self.assertListEqual(sink.received, [(1, 1)])