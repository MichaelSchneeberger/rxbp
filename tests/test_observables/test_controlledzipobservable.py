from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.selectionmsg import SelectCompleted, SelectNext
from rxbp.states.measuredstates.controlledzipstates import ControlledZipStates
from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.testing.testcasebase import TestCaseBase
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testobserver import TestObserver
from rxbp.testing.testscheduler import TestScheduler


class TestControlledZipObservable(TestCaseBase):
    """
    """

    class Command:
        pass

    class Go(Command):
        pass

    class Stop(Command):
        pass

    def setUp(self):
        self.scheduler = TestScheduler()
        self.left = TestObservable()
        self.right = TestObservable()
        self.exception = Exception()

    def measure_state(self, obs: ControlledZipObservable):
        return obs.state.get_measured_state(obs.termination_state)

    def measure_termination_state(self, obs: ControlledZipObservable):
        return obs.termination_state.get_measured_state()

    def test_init_state(self):
        sink = TestObserver()
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)

    def test_left_complete(self):
        """
                         s1.on_completed
        WaitOnLeftRight -----------------> Stopped
         InitState                   LeftCompletedState
        """

        sink = TestObserver()
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))

        self.left.on_completed()

        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.Stopped)
        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.LeftCompletedState)
        self.assertTrue(sink.is_completed)

    def test_wait_on_left_right_to_wait_on_right_with_synchronous_ack(self):
        """
                         s1.on_next
        WaitOnLeftRight ------------> WaitOnRight
         InitState                    InitState
        """

        sink = TestObserver()
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))

        ack1 = self.left.on_next_single(1)

        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnRight)
        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.InitState)
        self.assertFalse(ack1.has_value)
        self.assertListEqual(sink.received, [])

    def test_wait_on_right_to_wait_on_left_right_with_synchronous_ack(self):
        """
                     s2.on_next
        WaitOnRight ------------> WaitOnLeftRight
        """

        sink = TestObserver()
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))
        ack1 = self.left.on_next_single(1)

        ack2 = self.right.on_next_single(1)

        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)
        self.assertIsInstance(ack1.value, ContinueAck)
        self.assertIsInstance(ack2, ContinueAck)
        self.assertListEqual(sink.received, [(1, 1)])

    def test_wait_on_right_to_wait_on_right_with_synchronous_ack(self):
        """
                     s2.on_next
        WaitOnRight ------------> WaitOnRight
        """

        sink = TestObserver()
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))
        ack1 = self.left.on_next_single(2)

        ack2 = self.right.on_next_single(1)

        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnRight)
        self.assertFalse(ack1.has_value)
        self.assertIsInstance(ack2, ContinueAck)
        self.assertListEqual(sink.received, [])

    def test_wait_on_right_to_wait_on_left_with_synchronous_ack(self):
        """
                     s2.on_next
        WaitOnRight ------------> WaitOnLeftRight
        """

        sink = TestObserver()
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))
        ack1 = self.left.on_next_single(1)

        ack2 = self.right.on_next_single(2)

        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeft)
        self.assertIsInstance(ack1.value, ContinueAck)
        self.assertFalse(ack2.has_value)
        self.assertListEqual(sink.received, [])

    def test_wait_on_right_to_wait_on_left_right_multiple_elem_with_synchronous_ack(self):
        """
                     s2.on_next
        WaitOnRight ------------> WaitOnLeftRight
        """

        sink = TestObserver()
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))
        ack1 = self.left.on_next_list([1, 1, 2])

        ack2 = self.right.on_next_list([1, 2])

        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)
        self.assertIsInstance(ack1.value, ContinueAck)
        self.assertIsInstance(ack2, ContinueAck)
        self.assertListEqual(sink.received, [(1, 1), (2, 2)])

    def test_acknowledge_both(self):
        """
                        ack.on_next
        WaitOnRightLeft ------------> WaitOnRightLeft
        """

        sink = TestObserver(immediate_continue=0)
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))

        ack1 = self.left.on_next_list([1])
        ack2 = self.right.on_next_list([1])
        sink.ack.on_next(continue_ack)

        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)
        self.assertIsInstance(ack1.value, ContinueAck)
        self.assertIsInstance(ack2.value, ContinueAck)
        self.assertListEqual(sink.received, [(1, 1)])

    def test_left_complete_to_stopped(self):
        """
                    s2.on_next
        WaitOnRight ------------> Stopped
        LeftComplete              BothCompletedState
        """

        sink = TestObserver(immediate_continue=0)
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))
        self.left.on_next_list([1])
        self.left.on_completed()

        self.right.on_next_list([1])

        self.assertIsInstance(self.measure_termination_state(obs), TerminationStates.LeftCompletedState)
        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.Stopped)
        self.assertListEqual(sink.received, [(1, 1)])

    def test_exception(self):
        """
                    ack.on_next
        WaitOnRight ------------> Stopped
        """

        sink = TestObserver(immediate_continue=0)
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))

        ack1 = self.left.on_next_list([1])
        self.right.on_error(self.exception)

        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.Stopped)
        self.assertEqual(self.exception, sink.exception)

    def test_asynchronous_ack_from_selectors(self):
        """
                         ack.on_next
        WaitOnLeftRight ------------> WaitOnLeftRight
        """

        sink = TestObserver(immediate_continue=0)
        left_sel_sink = TestObserver(immediate_continue=0)
        right_sel_sink = TestObserver(immediate_continue=0)
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))
        obs.left_selector.observe(ObserverInfo(left_sel_sink))
        obs.right_selector.observe(ObserverInfo(right_sel_sink))
        ack1 = AckSubject()
        ack2 = AckSubject()
        self.left.on_next_single(1).subscribe(ack1)
        self.right.on_next_single(1).subscribe(ack2)
        sink.ack.on_next(continue_ack)

        self.assertFalse(ack1.has_value)

        left_sel_sink.ack.on_next(continue_ack)

        self.assertTrue(ack1.has_value)
        self.assertFalse(ack2.has_value)

        right_sel_sink.ack.on_next(continue_ack)

        self.assertTrue(ack1.has_value)
        self.assertTrue(ack2.has_value)

    def test_select_message_if_no_two_elements_match(self):
        """
                         ack.on_next
        WaitOnLeftRight ------------> WaitOnLeft
        """

        sink = TestObserver(immediate_continue=0)
        left_sel_sink = TestObserver(immediate_continue=0)
        right_sel_sink = TestObserver(immediate_continue=0)
        ack1 = AckSubject()
        ack2 = AckSubject()
        obs = ControlledZipObservable(
            left=self.left, right=self.right, scheduler=self.scheduler,
            request_left=lambda left, right: left <= right,
            request_right=lambda left, right: right <= left,
            match_func=lambda left, right: left == right,
        )
        obs.observe(ObserverInfo(sink))
        obs.left_selector.observe(ObserverInfo(left_sel_sink))
        obs.right_selector.observe(ObserverInfo(right_sel_sink))

        self.left.on_next_single(1).subscribe(ack1)
        self.right.on_next_single(2).subscribe(ack2)

        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeft)
        self.assertIsInstance(left_sel_sink.received[0], SelectCompleted)

        self.left.on_next_single(2).subscribe(ack1)

        self.assertIsInstance(self.measure_state(obs), ControlledZipStates.WaitOnLeftRight)

        for instance_obj, class_obj in zip(left_sel_sink.received, [SelectCompleted, SelectNext, SelectCompleted]):
            self.assertIsInstance(instance_obj, class_obj)

