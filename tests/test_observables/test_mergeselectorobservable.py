import unittest

from rxbp.ack.continueack import ContinueAck
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.observables.mergeselectorobservable import MergeSelectorObservable
from rxbp.selectors.selectionmsg import select_next, select_completed
from rxbp.states.measuredstates.controlledzipstates import ControlledZipStates
from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestMergeSelectorObservable(unittest.TestCase):
    def setUp(self) -> None:
        self.left = TestObservable()
        self.right = TestObservable()
        self.scheduler = TestScheduler()
        self.obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        self.exception = Exception()

    def measure_state(self, obs: MergeSelectorObservable):
        return obs.state.get_measured_state(obs.termination_state)

    def measure_termination_state(self, obs: MergeSelectorObservable):
        return obs.termination_state.get_measured_state()

    def test_initialize(self):
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)

    def test_state_waitonleftright_to_waitonleftright_with_leftonnext(self):
        """
                           left.on_next
        WaitOnLeftRight -----------------> WaitOnLeftRight
         InitState                             InitState
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))

        ack = self.left.on_next_list([select_completed])

        self.assertIsInstance(ack, ContinueAck)
        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)
        self.assertEqual([select_completed], sink.received)

    def test_multiple_left_select_complete_should_not_wait_on_right(self):
        """
                           left.on_next
        WaitOnLeftRight -----------------> WaitOnLeftRight
         InitState                             InitState
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))

        ack = self.left.on_next_list([select_completed, select_completed, select_completed])

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)
        self.assertEqual([select_completed, select_completed, select_completed], sink.received)
        self.assertIsInstance(ack, ContinueAck)

    def test_wait_on_left_right_to_wait_on_right(self):
        """
                         left.on_next
        WaitOnLeftRight -----------------> WaitOnRight
         InitState                          InitState
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))

        ack = self.left.on_next_list([select_next, select_completed])

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnRight)
        self.assertEqual([], sink.received)
        self.assertFalse(ack.is_sync)

    def test_wait_on_right_to_wait_on_left_right(self):
        """
                        right.on_next
        WaitOnRight -----------------> WaitOnLeftRight
         InitState                          InitState
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))
        ack_left = self.left.on_next_list([select_next, select_completed])

        ack_right = self.right.on_next_list([select_completed])

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)
        self.assertEqual([select_completed], sink.received)
        self.assertIsInstance(ack_left.value, ContinueAck)
        self.assertIsInstance(ack_right, ContinueAck)

    def test_select_same_element_multiple_times(self):
        """
                        right.on_next
        WaitOnRight -----------------> WaitOnLeftRight
         InitState                          InitState
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))
        ack_left = self.left.on_next_list([select_next, select_completed])

        ack_right = self.right.on_next_list([select_next, select_next, select_completed])

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)
        self.assertEqual([select_next, select_next, select_completed], sink.received)
        self.assertIsInstance(ack_left.value, ContinueAck)
        self.assertIsInstance(ack_right, ContinueAck)

    def test_state_waitonleft_to_waitonleftright(self):
        """
                        left.on_next
        WaitOnLeft -----------------> WaitOnLeftRight
         InitState                       InitState
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))
        ack_right = self.right.on_next_list([select_next, select_completed])

        ack_left = self.left.on_next_list([select_next, select_completed, select_completed])

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)
        self.assertEqual([select_next, select_completed, select_completed], sink.received)
        self.assertIsInstance(ack_left, ContinueAck)
        self.assertIsInstance(ack_right.value, ContinueAck)

    def test_state_waitonleft_to_waitonleft(self):
        """
                        left.on_next
        WaitOnLeft ---------------------> WaitOnLeft
         InitState                          InitState
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))
        ack_right = self.right.on_next_list([select_next, select_completed])

        ack_left = self.left.on_next_list([select_completed, select_completed])

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeft)
        self.assertEqual([select_completed, select_completed], sink.received)
        self.assertIsInstance(ack_left, ContinueAck)
        self.assertFalse(ack_right.has_value)

    def test_drain_left_select_completed(self):
        """
                    right.on_next
        WaitOnRight -----------------> WaitOnLeftRight
         InitState                          InitState
        """

        sink = TestObserver()
        obs = MergeSelectorObservable(
            left=self.left,
            right=self.right,
            scheduler=self.scheduler,
        )
        obs.observe(ObserverInfo(sink))
        ack_left = self.left.on_next_list([select_next, select_completed, select_completed])

        ack_right = self.right.on_next_list([select_completed])

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)
        self.assertEqual([select_completed, select_completed], sink.received)
        self.assertIsInstance(ack_left.value, ContinueAck)
        self.assertIsInstance(ack_right, ContinueAck)

    def test_right_on_completed(self):
        """
                    right.on_completed               left.on_next
        WaitOnLeft --------------------> WaitOnLeft ------------------> Stopped
        """

        sink = TestObserver(immediate_continue=0)
        self.obs.observe(ObserverInfo(sink))
        self.right.on_next_list([select_completed])

        self.right.on_completed()

        self.assertIsInstance(self.measure_state(self.obs), ControlledZipStates.WaitOnLeft)

        self.left.on_next_list([select_next])

        self.assertIsInstance(self.measure_state(self.obs), ControlledZipStates.Stopped)
        self.assertEqual([], sink.received)     # the last SelectCompleted is optional

    def test_exception(self):
        """
                    right.on_error
        WaitOnRight -------------> Stopped
        """

        sink = TestObserver(immediate_continue=0)
        self.obs.observe(ObserverInfo(sink))
        ack1 = self.left.on_next_list([select_completed])

        self.right.on_error(self.exception)

        self.assertIsInstance(self.measure_state(self.obs), ControlledZipStates.Stopped)
        self.assertEqual(self.exception, sink.exception)
