import threading
from abc import ABC, abstractmethod
from typing import Callable, Any, Iterator, Optional

from rx.disposable import CompositeDisposable
from rxbp.ack.ackimpl import continue_ack, stop_ack
from rxbp.ack.ackbase import AckBase
from rxbp.ack.acksubject import AckSubject
from rxbp.ack.merge import _merge

from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.selectionmsg import select_next, select_completed
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.typing import ElementType


class ControlledZipObservable(Observable):
    def __init__(self, left: Observable, right: Observable,
                 request_left: Callable[[Any, Any], bool],
                 request_right: Callable[[Any, Any], bool],
                 match_func: Callable[[Any, Any], bool],
                 scheduler: Scheduler):
        """
        :param left: lef observable
        :param right: right observable
        :param is_lower: if right is lower than left, request next right
        :param is_higher: if right is higher than left, request next left
        """

        # save input arguments
        self.left_observable = left
        self.right_observable = right
        self.request_left = request_left
        self.request_right = request_right
        self.match_func = match_func

        # create two selector observablesubjects used to match Flowables
        self.left_selector = PublishOSubject(scheduler=scheduler)
        self.right_selector = PublishOSubject(scheduler=scheduler)

        self.lock = threading.RLock()

        # ControlledZipObservable state
        self.observer = None
        self.termination_state = ControlledZipObservable.InitState()
        self.zip_state = ControlledZipObservable.WaitOnLeftRight()

    class TerminationState(ABC):
        """ The zip observable actor state is best captured with two states, to avoid having to
        enumerate a big number of states. The termination state is changed on the `on_complete`
        or `on_error` method call.

        The ZipState depends on the TerminationState but not vice versa.
        """

        @abstractmethod
        def get_current_state(self):
            ...

    class InitState(TerminationState):
        def get_current_state(self):
            return self

    class LeftCompletedState(TerminationState, ABC):
        pass

    class RightCompletedState(TerminationState, ABC):
        pass

    class BothCompletedState(LeftCompletedState, RightCompletedState):
        def get_current_state(self):
            return self

    class LeftCompletedStateImpl(LeftCompletedState):
        def __init__(self, raw_prev_state: Optional['ControlledZipObservable.TerminationState']):
            self.raw_prev_state = raw_prev_state

        def get_current_state(self):
            prev_state = self.raw_prev_state

            if isinstance(prev_state, ControlledZipObservable.InitState):
                return self
            elif isinstance(prev_state, ControlledZipObservable.RightCompletedState):
                return ControlledZipObservable.BothCompletedState()
            else:
                return self.raw_prev_state

    class RightCompletedStateImpl(RightCompletedState):
        def __init__(self, raw_prev_state: Optional['ControlledZipObservable.TerminationState']):
            self.raw_prev_state = raw_prev_state

        def get_current_state(self):
            prev_state = self.raw_prev_state

            if isinstance(prev_state, ControlledZipObservable.InitState):
                return self
            elif isinstance(prev_state, ControlledZipObservable.LeftCompletedState):
                return ControlledZipObservable.BothCompletedState()
            else:
                return self.raw_prev_state

    class ErrorState(TerminationState):
        def __init__(self, raw_prev_state: Optional['ControlledZipObservable.TerminationState'],
                     ex: Exception):
            self.raw_prev_state = raw_prev_state
            self.ex = ex

        def get_current_state(self):
            prev_state = self.raw_prev_state

            if isinstance(prev_state, ControlledZipObservable.InitState):
                return self
            else:
                return self.raw_prev_state

    class ControlledZipState(ABC):
        """ The state of the controlled zip observable actor

        The true state (at the point when it is read) is only known by calling  the
        `get_measured_state` method.
        """

        @abstractmethod
        def get_current_state(self, final_state: 'ControlledZipObservable.TerminationState'):
            ...

    class WaitOnLeft(ControlledZipState):
        def __init__(self,
                     right_val: Any,
                     right_iter: Iterator,
                     right_in_ack: AckBase,
                     right_sel_ack: Optional[AckBase]
                     ):
            self.right_val = right_val
            self.right_iter = right_iter
            self.right_in_ack = right_in_ack
            self.right_sel_ack = right_sel_ack

        def get_current_state(self, final_state: 'ControlledZipObservable.TerminationState'):
            if isinstance(final_state, ControlledZipObservable.LeftCompletedState) or \
                    isinstance(final_state, ControlledZipObservable.ErrorState):
                return ControlledZipObservable.Stopped()
            else:
                return self

    class WaitOnRight(ControlledZipState):
        def __init__(self,
                     left_iter: Iterator,
                     left_val: Any,
                     left_in_ack: AckBase,
                     left_sel_ack: Optional[AckBase]):
            self.left_val = left_val
            self.left_iter = left_iter
            self.left_in_ack = left_in_ack
            self.left_sel_ack = left_sel_ack

        def get_current_state(self, final_state: 'ControlledZipObservable.TerminationState'):
            if isinstance(final_state, ControlledZipObservable.RightCompletedState) or \
                    isinstance(final_state, ControlledZipObservable.ErrorState):
                return ControlledZipObservable.Stopped()
            else:
                return self


    class WaitOnLeftRight(ControlledZipState):
        """ Zip observable actor has or will back-pressure the left and right source, but
        no element has yet been received.

        In this state, the left and right buffer are empty.
        """

        def get_current_state(self, final_state: 'ControlledZipObservable.TerminationState'):
            if isinstance(final_state, ControlledZipObservable.InitState):
                return self
            else:
                return ControlledZipObservable.Stopped()

    class Transition(ControlledZipState):
        """ Zip observable actor is zipping the values just received by a source and
         from the buffer.

        In this state the actual termination state is ignored in the `get_measured_state`
        method.
        """

        def __init__(self, is_left: bool, ack: AckBase, iter: Iterator, val: Any,
                     prev_state: 'ControlledZipObservable.ControlledZipState',
                     prev_terminal_state: 'ControlledZipObservable.TerminationState'):
            """
            :param is_left:
            :param ack:
            :param iter:
            """

            self.is_left = is_left
            self.ack = ack
            self.val = val
            self.iter = iter

            self.prev_state = prev_state
            self.prev_terminal_state = prev_terminal_state

        def get_current_state(self, final_state: 'ControlledZipObservable.TerminationState'):
            if isinstance(self.prev_state, ControlledZipObservable.Stopped):
                return self.prev_state

            # Needed for `signal_on_complete_or_on_error`
            elif isinstance(self.prev_state, ControlledZipObservable.WaitOnLeftRight):
                if self.is_left:
                    return ControlledZipObservable.WaitOnRight(left_in_ack=self.ack,
                                                               left_val=self.val,
                                                               left_iter=self.iter,
                                                               left_sel_ack=None) \
                        .get_current_state(final_state=self.prev_terminal_state)
                else:
                    return ControlledZipObservable.WaitOnLeft(right_in_ack=self.ack,
                                                              right_val=self.val,
                                                              right_iter=self.iter,
                                                              right_sel_ack=None) \
                        .get_current_state(final_state=self.prev_terminal_state)

            else:
                return self

    class Stopped(ControlledZipState):
        def get_current_state(self, final_state: 'ControlledZipObservable.TerminationState'):
            return self

    def _iterate_over_batch(self, elem: ElementType, is_left: bool) \
            -> AckBase:
        """ This function is called once elements are received from left and right observable

        Loop over received elements. Send elements downstream if the match function applies. Request new elements
        from left, right or left and right observable.

        :param elem: either elements received form left or right observable depending on is_left argument
        :param is_left: if True then the _iterate_over_batch is called on left elements received

        :return: acknowledgment that will be returned from `on_next` called from either left or right observable
        """

        upstream_ack = AckSubject()

        iterable = iter(elem)
        val = next(iterable)

        # not a concurrent situation, therefore, read and write states without a lock
        raw_prev_termination_state = self.termination_state
        prev_terminal_state = self.termination_state.get_current_state()
        prev_state = self.zip_state.get_current_state(final_state=prev_terminal_state)
        next_state = ControlledZipObservable.Transition(is_left=is_left, ack=upstream_ack, iter=iterable,
                                                        val=val, prev_state=prev_state,
                                                        prev_terminal_state=prev_terminal_state)
        self.zip_state = next_state

        if isinstance(prev_state, ControlledZipObservable.Stopped):
            return stop_ack
        elif isinstance(prev_state, ControlledZipObservable.WaitOnLeftRight):
            return upstream_ack
        elif is_left and isinstance(prev_state, ControlledZipObservable.WaitOnLeft):
            left_val = val
            left_iter = iterable
            left_in_ack = upstream_ack
            last_left_sel_ack = None
            right_val = prev_state.right_val
            right_iter = prev_state.right_iter
            right_in_ack = prev_state.right_in_ack
            other_upstream_ack = prev_state.right_in_ack
            last_right_sel_ack = prev_state.right_sel_ack
        elif not is_left and isinstance(prev_state, ControlledZipObservable.WaitOnRight):
            left_val = prev_state.left_val
            left_iter = prev_state.left_iter
            left_in_ack = prev_state.left_in_ack
            last_left_sel_ack = prev_state.left_sel_ack
            right_val = val
            right_iter = iterable
            right_in_ack = upstream_ack
            other_upstream_ack = prev_state.left_in_ack
            last_right_sel_ack = None
        else:
            raise Exception('unknown state "{}", is_left {}'.format(prev_state, is_left))

        # keep elements to be sent in a buffer. Only when the incoming batch of elements is iterated over, the
        # elements in the buffer are sent.
        left_index_buffer = []                  # index of the elements from the left observable that got selected
        right_index_buffer = []                 #   by the match function
        zipped_output_buffer = []

        do_back_pressure_left = False
        do_back_pressure_right = False

        # iterate over two batches of elements from left and right observable until one batch is empty
        while True:

            # print('left_val={}, right_val={}'.format(left_val, right_val))

            if self.match_func(left_val, right_val):
                left_index_buffer.append(select_next)
                right_index_buffer.append(select_next)

                # add to buffer
                zipped_output_buffer.append((left_val, right_val))

            # save old value before left_val is overwritten
            old_left_val = left_val

            request_left = self.request_left(left_val, right_val)
            if request_left:
                left_index_buffer.append(select_completed)
                try:
                    left_val = next(left_iter)
                except StopIteration:
                    do_back_pressure_left = True

            if self.request_right(old_left_val, right_val):
                right_index_buffer.append(select_completed)

                try:
                    right_val = next(right_iter)
                except StopIteration:
                    do_back_pressure_right = True
                    break
            elif not request_left:
                raise Exception('either `request_left` or `request_right` condition needs to be True')

            if do_back_pressure_left:
                break

        # only send elements downstream, if there are any to be sent
        if zipped_output_buffer:
            # # create a generate function to be sent
            # def gen():
            #     yield from zipped_output_buffer

            zip_out_ack = self.observer.on_next(zipped_output_buffer)
        else:
            zip_out_ack = continue_ack

        # only send elements over the left selector observer, if there are any to be sent
        if left_index_buffer:
            # def gen():
            #     yield from left_index_buffer

            left_out_ack = self.left_selector.on_next(left_index_buffer)
        else:
            left_out_ack = last_left_sel_ack or continue_ack

        # only send elements over the right selector observer, if there are any to be sent
        if right_index_buffer:
            # def gen():
            #     yield from right_index_buffer

            right_out_ack = self.right_selector.on_next(right_index_buffer)
        else:
            right_out_ack = last_right_sel_ack or continue_ack

        # all elements in the left and right iterable are send downstream
        if do_back_pressure_left and do_back_pressure_right:
            next_state = ControlledZipObservable.WaitOnLeftRight()

        elif do_back_pressure_left:
            next_state = ControlledZipObservable.WaitOnLeft(right_val=right_val, right_iter=right_iter,
                                                            right_in_ack=right_in_ack,
                                                            right_sel_ack=right_out_ack)

        elif do_back_pressure_right:
            next_state = ControlledZipObservable.WaitOnRight(left_val=left_val, left_iter=left_iter,
                                                             left_in_ack=left_in_ack,
                                                             left_sel_ack=left_out_ack)

        else:
            raise Exception('at least one side should be back-pressured')

        with self.lock:
            # get termination state
            raw_prev_termination_state = self.termination_state

            # set next state
            self.zip_state = next_state

        prev_termination_state = raw_prev_termination_state.get_current_state()

        def stop_active_acks():
            if isinstance(last_right_sel_ack, AckSubject): #last_right_sel_ack is not None :
                last_right_sel_ack.on_next(stop_ack)
            elif isinstance(last_left_sel_ack, AckSubject): #last_left_sel_ack is not None:
                last_left_sel_ack.on_next(stop_ack)
            other_upstream_ack.on_next(stop_ack)

        # stop back-pressuring both sources, because there is no need to request elements
        # from completed source
        if isinstance(prev_termination_state, ControlledZipObservable.LeftCompletedState) \
                and do_back_pressure_left:

            # current state should be Stopped
            assert isinstance(next_state.get_current_state(prev_termination_state), ControlledZipObservable.Stopped)

            self._signal_on_complete_or_on_error(raw_state=next_state)
            stop_active_acks()
            return stop_ack

        # stop back-pressuring both sources, because there is no need to request elements
        # from completed source
        elif isinstance(prev_termination_state, ControlledZipObservable.RightCompletedState) \
                and do_back_pressure_right:
            self._signal_on_complete_or_on_error(raw_state=next_state)
            stop_active_acks()
            return stop_ack

        # in error state, stop back-pressuring both sources
        elif isinstance(prev_termination_state, ControlledZipObservable.ErrorState):
            self._signal_on_complete_or_on_error(raw_state=next_state, ex=prev_termination_state.ex)
            stop_active_acks()
            return stop_ack

        # finish connecting ack only if not in Stopped or Error state
        else:

            if do_back_pressure_left and do_back_pressure_right:

                # integrate selector acks
                # result_ack_left = zip_out_ack.merge_ack(left_out_ack)
                # result_ack_right = zip_out_ack.merge_ack(right_out_ack)
                result_ack_left = _merge(zip_out_ack, left_out_ack)
                result_ack_right = _merge(zip_out_ack, right_out_ack)

                # directly return ack depending on whether left or right called `iterate_over_batch`
                if is_left:
                    result_ack_right.subscribe(right_in_ack)
                    return result_ack_left
                else:
                    result_ack_left.subscribe(left_in_ack)
                    return result_ack_right

            # all elements in the left buffer are send to the observer, back-pressure only left
            elif do_back_pressure_left:

                # result_left_ack = zip_out_ack.merge_ack(left_out_ack)
                result_left_ack = _merge(zip_out_ack, left_out_ack)
                if is_left:
                    return result_left_ack
                else:
                    result_left_ack.subscribe(left_in_ack)
                    return right_in_ack

            # all elements in the left buffer are send to the observer, back-pressure only right
            elif do_back_pressure_right:

                # result_right_ack = zip_out_ack.merge_ack(right_out_ack)
                result_right_ack = _merge(zip_out_ack, right_out_ack)
                if is_left:
                    result_right_ack.subscribe(right_in_ack)
                    return left_in_ack
                else:
                    return result_right_ack

            else:
                raise Exception('illegal case')

    def _on_next_left(self, elem: ElementType):

        try:
            return_ack = self._iterate_over_batch(elem=elem, is_left=True)
        except Exception as exc:
            self.observer._on_error(exc)
            return stop_ack
        return return_ack

    def _on_next_right(self, elem: ElementType):

        try:
            return_ack = self._iterate_over_batch(elem=elem, is_left=False)
        except Exception as exc:
            self.observer._on_error(exc)
            return stop_ack
        return return_ack

    def _signal_on_complete_or_on_error(self, raw_state: 'ControlledZipObservable.ControlledZipState',
                                        ex: Exception = None):
        """ this function is called once, because 'on_complete' or 'on_error' are called once according to the rxbp
        convention

        :param raw_state: controlled zip state
        :param ex: catched exception to be forwarded downstream
        :return:
        """

        # stop active acknowledgments
        if isinstance(raw_state, ControlledZipObservable.WaitOnLeftRight):
            pass
        elif isinstance(raw_state, ControlledZipObservable.WaitOnLeft):
            raw_state.right_in_ack.on_next(stop_ack)
        elif isinstance(raw_state, ControlledZipObservable.WaitOnRight):
            raw_state.left_in_ack.on_next(stop_ack)
        else:
            pass

        # terminate observer
        if ex:
            self.observer.on_error(ex)
        else:
            self.observer.on_completed()

    def _on_error_or_complete(self, next_final_state: 'ControlledZipObservable.TerminationState'):

        with self.lock:
            raw_prev_final_state = self.termination_state
            raw_prev_state = self.zip_state
            next_final_state.raw_prev_state = raw_prev_final_state
            self.termination_state = next_final_state

        prev_final_state = raw_prev_final_state.get_current_state()
        prev_state = raw_prev_state.get_current_state(final_state=prev_final_state)

        curr_final_state = next_final_state.get_current_state()
        curr_state = raw_prev_state.get_current_state(final_state=curr_final_state)

        if not isinstance(prev_state, ControlledZipObservable.Stopped) \
                and isinstance(curr_state, ControlledZipObservable.Stopped):
            self._signal_on_complete_or_on_error(raw_prev_state)

    def _on_error(self, ex):
        next_final_state = ControlledZipObservable.ErrorState(raw_prev_state=None, ex=ex)
        self._on_error_or_complete(next_final_state=next_final_state)

    def _on_completed_left(self):
        next_final_state = ControlledZipObservable.LeftCompletedStateImpl(raw_prev_state=None)
        self._on_error_or_complete(next_final_state=next_final_state)

    def _on_completed_right(self):
        next_final_state = ControlledZipObservable.RightCompletedStateImpl(raw_prev_state=None)
        self._on_error_or_complete(next_final_state=next_final_state)

    def observe(self, observer_info: ObserverInfo):
        """ This function ought be called at most once.

        :param observer: downstream obseNonerver
        :return: Disposable
        """

        self.observer = observer_info.observer
        source = self

        class LeftControlledZipObserver(Observer):
            def on_next(self, elem: ElementType) -> AckBase:
                return source._on_next_left(elem)

            def on_error(self, exc: Exception):
                return source._on_error(exc)

            def on_completed(self):
                source._on_completed_left()

        class RightControlledZipObserver(Observer):
            def on_next(self, elem: ElementType) -> AckBase:
                return source._on_next_right(elem)

            def on_error(self, exc: Exception):
                return source._on_error(exc)

            def on_completed(self):
                source._on_completed_right()

        left_observer = LeftControlledZipObserver()
        left_subscription = observer_info.copy(left_observer)
        d1 = self.left_observable.observe(left_subscription)

        right_observer = RightControlledZipObserver()
        left_subscription = observer_info.copy(right_observer)
        d2 = self.right_observable.observe(left_subscription)

        return CompositeDisposable(d1, d2)
