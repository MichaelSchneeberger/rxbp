import threading
from typing import Callable, Any

from rx.disposable import CompositeDisposable

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import continue_ack
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.operators.merge import _merge
from rxbp.ack.stopack import stop_ack
from rxbp.observable import Observable
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.selectors.selectionmsg import select_next, select_completed
from rxbp.states.measuredstates.controlledzipstates import ControlledZipStates
from rxbp.states.measuredstates.terminationstates import TerminationStates
from rxbp.states.rawstates.rawcontrolledzipstates import RawControlledZipStates
from rxbp.states.rawstates.rawterminationstates import RawTerminationStates
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

        self.observer = None

        # state once observed
        self.termination_state = RawTerminationStates.InitState()
        self.state = RawControlledZipStates.WaitOnLeftRight(
            # left_sel=None,
            # right_sel=None,
        )

    def _iterate_over_batch(
            self,
            elem: ElementType,
            is_left: bool,
    ) -> AckMixin:
        """ This function is called once elements are received from left and right observable

        Loop over received elements. Send elements downstream if the match function applies. Request new elements
        from left, right or left and right observable.

        :param elem: either elements received form left or right observable depending on is_left argument
        :param is_left: if True then the _iterate_over_batch is called on left elements received

        :return: acknowledgment that will be returned from `on_next` called from either left or right observable
        """

        upstream_ack = AckSubject()

        iterable = iter(elem)

        # empty iterable
        try:
            val = next(iterable)
        except StopIteration:
            return continue_ack

        next_state = RawControlledZipStates.ZipElements(
            val=val,
            is_left=is_left,
            ack=upstream_ack,
            iter=iterable,
        )

        with self.lock:
            next_state.prev_raw_state = self.state
            next_state.prev_raw_termination_state = self.termination_state
            self.state = next_state

        raw_prev_termination_state = next_state.prev_raw_termination_state
        prev_raw_state = next_state.prev_raw_state
        prev_state = prev_raw_state.get_measured_state(raw_prev_termination_state)

        if isinstance(prev_state, ControlledZipStates.Stopped):
            return stop_ack

        elif isinstance(prev_state, ControlledZipStates.WaitOnLeftRight):
            return upstream_ack

        elif is_left and isinstance(prev_state, ControlledZipStates.WaitOnLeft):
            left_val = val
            left_iter = iterable
            left_in_ack = upstream_ack
            # last_left_sel_ack = None
            right_val = prev_state.right_val
            right_iter = prev_state.right_iter
            right_in_ack = prev_state.right_ack
            # last_right_sel_ack = prev_state.right_sel_ack
            other_upstream_ack = prev_state.right_ack

        elif not is_left and isinstance(prev_state, ControlledZipStates.WaitOnRight):
            left_val = prev_state.left_val
            left_iter = prev_state.left_iter
            left_in_ack = prev_state.left_ack
            # last_left_sel_ack = prev_state.left_sel_ack
            right_val = val
            right_iter = iterable
            right_in_ack = upstream_ack
            # last_right_sel_ack = None
            other_upstream_ack = prev_state.left_ack

        else:
            raise Exception('unknown state "{}", is_left {}'.format(prev_state, is_left))

        # keep elements to be sent in a buffer. Only when the incoming batch of elements is iterated over, the
        # elements in the buffer are sent.
        left_index_buffer = []                  # index of the elements from the left observable that got selected
        right_index_buffer = []                 # by the match function
        zipped_output_buffer = []

        request_new_elem_from_left = False
        request_new_elem_from_right = False

        # iterate over two batches of elements from left and right observable
        # until one batch is empty
        while True:

            if self.match_func(left_val, right_val):
                left_index_buffer.append(select_next)
                right_index_buffer.append(select_next)

                # add to buffer
                zipped_output_buffer.append((left_val, right_val))

            request_left = self.request_left(left_val, right_val)
            request_right = self.request_right(left_val, right_val)

            if request_left:
                left_index_buffer.append(select_completed)

                try:
                    left_val = next(left_iter)
                except StopIteration:
                    request_new_elem_from_left = True

            if request_right:
                right_index_buffer.append(select_completed)

                try:
                    right_val = next(right_iter)
                except StopIteration:
                    request_new_elem_from_right = True
                    break

            elif not request_left:
                raise Exception('either `request_left` or `request_right` condition needs to be True')

            if request_new_elem_from_left:
                break

        # only send elements downstream, if there are any to be sent
        if zipped_output_buffer:
            zip_out_ack = self.observer.on_next(zipped_output_buffer)
        else:
            zip_out_ack = continue_ack

        # only send elements over the left selector observer, if there are any to be sent
        if left_index_buffer:
            left_out_ack = self.left_selector.on_next(left_index_buffer)
        else:
            left_out_ack = continue_ack #last_left_sel_ack or continue_ack

        # only send elements over the right selector observer, if there are any to be sent
        if right_index_buffer:
            right_out_ack = self.right_selector.on_next(right_index_buffer)
        else:
            right_out_ack = continue_ack #last_right_sel_ack or continue_ack

        # all elements in the left and right iterable are send downstream
        if request_new_elem_from_left and request_new_elem_from_right:
            next_state = RawControlledZipStates.WaitOnLeftRight(
                # right_sel=right_sel,
                # left_sel=left_sel,
            )

        elif request_new_elem_from_left:
            next_state = RawControlledZipStates.WaitOnLeft(
                right_val=right_val,
                right_iter=right_iter,
                right_ack=right_in_ack,
                # right_sel=right_sel,
                # left_sel=left_sel,
                # right_sel_ack=right_out_ack,
            )

        elif request_new_elem_from_right:
            next_state = RawControlledZipStates.WaitOnRight(
                left_val=left_val,
                left_iter=left_iter,
                left_ack=left_in_ack,
                # right_sel=right_sel,
                # left_sel=left_sel,
                # left_sel_ack=left_out_ack,
            )

        else:
            raise Exception('at least one side should be back-pressured')

        with self.lock:
            # get termination state
            raw_prev_termination_state = self.termination_state

            # set next state
            self.state = next_state

        prev_termination_state = raw_prev_termination_state.get_measured_state()

        def stop_active_acks():
            # if isinstance(last_right_sel_ack, AckSubject):
            #     last_right_sel_ack.on_next(stop_ack)
            # elif isinstance(last_left_sel_ack, AckSubject):
            #     last_left_sel_ack.on_next(stop_ack)
            other_upstream_ack.on_next(stop_ack)

        # stop back-pressuring both sources, because there is no need to request elements
        # from completed source
        if isinstance(prev_termination_state, TerminationStates.LeftCompletedState) \
                and request_new_elem_from_left:

            self._signal_on_complete_or_on_error(state=next_state)
            stop_active_acks()
            return stop_ack

        # stop back-pressuring both sources, because there is no need to request elements
        # from completed source
        elif isinstance(prev_termination_state, TerminationStates.RightCompletedState) \
                and request_new_elem_from_right:
            self._signal_on_complete_or_on_error(state=next_state)
            stop_active_acks()
            return stop_ack

        # in error state, stop back-pressuring both sources
        elif isinstance(prev_termination_state, TerminationStates.ErrorState):
            self._signal_on_complete_or_on_error(state=next_state, ex=prev_termination_state.ex)
            stop_active_acks()
            return stop_ack

        # finish connecting ack only if not in Stopped or Error state
        else:

            # result_out_ack = AckSubject()
            # _merge(_merge(zip_out_ack, left_out_ack), right_out_ack).subscribe(result_out_ack)

            if request_new_elem_from_left and request_new_elem_from_right:

                # integrate selector acks
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
            elif request_new_elem_from_left:

                # result_ack_left = _merge(zip_out_ack, left_out_ack)

                # result_out_ack = AckSubject()
                # _merge(_merge(zip_out_ack, left_out_ack), right_out_ack).subscribe(result_out_ack)
                result_out_ack = _merge(_merge(zip_out_ack, left_out_ack), right_out_ack)

                if is_left:
                    return result_out_ack

                else:
                    result_out_ack.subscribe(left_in_ack)
                    return right_in_ack

            # all elements in the left buffer are send to the observer, back-pressure only right
            elif request_new_elem_from_right:

                # result_ack_right = _merge(zip_out_ack, left_out_ack)

                # result_out_ack = AckSubject()
                # _merge(_merge(zip_out_ack, left_out_ack), right_out_ack).subscribe(result_out_ack)
                result_out_ack = _merge(_merge(zip_out_ack, left_out_ack), right_out_ack)

                if is_left:
                    result_out_ack.subscribe(right_in_ack)
                    return left_in_ack

                else:
                    return result_out_ack

            else:
                raise Exception('illegal case')

    def _on_next_left(self, elem: ElementType):

        try:
            return_ack = self._iterate_over_batch(elem=elem, is_left=True)
        except Exception as exc:
            # self.observer._on_error(exc)
            self._on_error(exc)
            return stop_ack
        return return_ack

    def _on_next_right(self, elem: ElementType):
        try:
            return_ack = self._iterate_over_batch(elem=elem, is_left=False)
        except Exception as exc:
            # self.observer._on_error(exc)
            self._on_error(exc)
            return stop_ack
        return return_ack

    def _signal_on_complete_or_on_error(
            self,
            state: ControlledZipStates.ZipState,
            exc: Exception = None,
    ):
        """ this function is called once, because 'on_complete' or 'on_error' are called once according to the rxbp
        convention

        :param state: controlled join_flowables state
        :param ex: catched exception to be forwarded downstream
        :return:
        """

        # stop active acknowledgments
        if isinstance(state, ControlledZipStates.WaitOnLeftRight):
            pass
        elif isinstance(state, ControlledZipStates.WaitOnLeft):
            state.right_in_ack.on_next(stop_ack)
        elif isinstance(state, ControlledZipStates.WaitOnRight):
            state.left_in_ack.on_next(stop_ack)
        else:
            pass

        # terminate observer
        if exc:
            self.observer.on_error(exc)
        else:
            self.observer.on_completed()

    def _on_error_or_complete(
            self,
            next_final_state: RawTerminationStates.TerminationState,
            exc: Exception = None,
    ):

        with self.lock:
            raw_prev_final_state = self.termination_state
            raw_prev_state = self.state
            next_final_state.raw_prev_state = raw_prev_final_state
            self.termination_state = next_final_state

        prev_state = raw_prev_state.get_measured_state(
            raw_termination_state=raw_prev_final_state,
        )

        curr_state = raw_prev_state.get_measured_state(
            raw_termination_state=next_final_state,
        )

        if not isinstance(prev_state, ControlledZipStates.Stopped) \
                and isinstance(curr_state, ControlledZipStates.Stopped):
            self._signal_on_complete_or_on_error(raw_prev_state, exc=exc)

    def _on_error(self, exc):
        next_final_state = RawTerminationStates.ErrorState(
            exc=exc,
        )
        self._on_error_or_complete(
            next_final_state=next_final_state,
            exc=exc,
        )

    def _on_completed_left(self):
        next_final_state = RawTerminationStates.LeftCompletedState()
        self._on_error_or_complete(next_final_state=next_final_state)

    def _on_completed_right(self):
        next_final_state = RawTerminationStates.RightCompletedState()
        self._on_error_or_complete(next_final_state=next_final_state,)

    def observe(self, observer_info: ObserverInfo):
        """ This function ought be called at most once.

        :param observer: downstream obseNonerver
        :return: Disposable
        """

        self.observer = observer_info.observer
        source = self

        class LeftControlledZipObserver(Observer):
            def on_next(self, elem: ElementType) -> AckMixin:
                return source._on_next_left(elem)

            def on_error(self, exc: Exception):
                return source._on_error(exc)

            def on_completed(self):
                source._on_completed_left()

        class RightControlledZipObserver(Observer):
            def on_next(self, elem: ElementType) -> AckMixin:
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
