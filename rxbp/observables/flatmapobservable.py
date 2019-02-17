from typing import Callable, Any, Optional, List, Iterator

from rx import config
from rx.core import Disposable

from rxbp.ack import Ack, Continue, Stop, stop_ack, continue_ack
from rxbp.observable import Observable
from rxbp.observer import Observer


class FlatMapObservable(Observable):
    def __init__(self, source, selector: Callable[[Any], Observable], delay_errors=False):
        self.source = source
        self.selector = selector
        self.delay_errors = delay_errors

    def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
        source = self

        class State:
            pass

        class WaitOnNextChild(State):
            def __init__(self, ack=None):
                self.ack = ack

        class WaitOnActiveChild(State):
            # this state is necessary in case the inner observable completes before state is set to `Active`; in this
            #   case, return the last ack returned by output observer
            def __init__(self, outer_iter):
                self.outer_iter = outer_iter

        class Active(State):
            def __init__(self, outer_iter, disposable: List[Disposable]):
                self.outer_iter = outer_iter
                self.disposable = disposable

        class WaitOuterOnCompleteOrOnError(State):
            # outer observer does not complete the output observer (except in WaitOnNextChild); however, it signals
            #   that output observer should be completed if inner observer completes
            def __init__(self, ex, disposable: Disposable = None):
                self.ex = ex
                self.disposable = disposable

        class Completed(State):
            pass

        # is_active = [True]
        state = [WaitOnNextChild(Continue())]
        lock = config['concurrency'].RLock()
        # observer_completed = [False]

        def cancel_state():
            # with lock:
            #     observer_completed[0] = True
            pass

        def report_invalid_state(state: State, method: str):
            cancel_state()
            scheduler.report_failure(
                Exception('State {} in the ConcatMap.{} implementation is invalid'.format(state, method)))

        class ConcatMapObserver(Observer):
            def __init__(self):
                self.errors = [] if source.delay_errors else None

            def on_next(self, outer_elem):
                # by convention, we get a single element, e.g. no iterable
                with lock:
                    # blindly set state to WaitOnActiveChild, analyse current state later
                    previous_state = state[0]
                    state[0] = WaitOnActiveChild(outer_iter=None)

                # nothing happened
                if isinstance(previous_state, WaitOnNextChild):
                    pass

                # when should this happen?
                elif isinstance(previous_state, Completed):
                    state[0] = previous_state
                    return stop_ack

                else:
                    report_invalid_state(previous_state, 'on_next (1)')
                    return stop_ack

                # select child observable from received item
                child = source.selector(outer_elem)

                # create a new child observer, and subscribe it to child observable
                async_upstream_ack = Ack()
                child_observer = ChildObserver(observer, scheduler, async_upstream_ack, self)
                disposable = child.unsafe_subscribe(child_observer, scheduler, subscribe_scheduler)

                with lock:
                    # blindly set state to Active, analyse current state later
                    previous_state = state[0]
                    state[0] = Active(None, disposable)

                # possible transitions during child subscription:
                # - WaitOnActiveChild -> nothing happened
                # - WaitOnNextChild -> inner observable completed
                # - Completed -> inner observable got an error

                # inner observable completed during subscription
                if isinstance(previous_state, WaitOnNextChild):
                    with lock:
                        state[0] = previous_state
                    return previous_state.ack

                # no state transition happened during child subscription
                elif isinstance(previous_state, WaitOnActiveChild):
                    return async_upstream_ack

                elif isinstance(previous_state, Completed):
                    return stop_ack

                else:
                    report_invalid_state(previous_state, 'on_next (2)')
                    return stop_ack

            def on_error(self, exc):
                with lock:
                    previous_state = state[0]
                    state[0] = WaitOuterOnCompleteOrOnError(exc)

                # only if state is WaitOnNextChild, complete observer
                if isinstance(previous_state, WaitOnNextChild): # and not observer_completed[0]: todo: is this necessary
                    # calls to root.on_next and root.on_error or root.on_completed happen in order,
                    # therefore state is not changed concurrently in WaitOnNextChild
                    # if not observer_completed[0]: # todo: remove this?
                    observer.on_error(exc)
                    # observer_completed[0] = True

                    with lock:
                        state[0] = Completed()

            def on_completed(self):
                with lock:
                    previous_state = state[0]
                    state[0] = WaitOuterOnCompleteOrOnError(None)

                # print('complete outer "{}"'.format(previous_state))

                if isinstance(previous_state, WaitOnNextChild): # and not observer_completed[0]:
                    # calls to root.on_next and root.on_completed happen in order,
                    # therefore state is not changed concurrently at the WaitOnNextChild
                    # if not observer_completed[0]: # todo: remove this?
                    observer.on_completed()
                    # observer_completed[0] = True
                    state[0] = Completed()

        class ChildObserver(Observer):
            def __init__(self, out: Observer, scheduler, async_upstream_ack: Ack, concat_observer):
                self.out = out
                self.scheduler = scheduler
                self.async_upstream_ack = async_upstream_ack
                self.concat_observer = concat_observer
                self.ack = Continue()

                self.completed = False
                self.exception = None

            def on_next(self, v):
                # on_next, on_completed, on_error are called ordered/non-concurrently
                ack = self.out.on_next(v)

                # if ack==Stop, then also update outer observer
                if isinstance(ack, Stop):
                    with lock:
                        state[0] = Completed()
                elif not isinstance(v, Continue):
                    def _(v):
                        if isinstance(v, Stop):
                            with lock:
                                state[0] = Completed()
                    ack.subscribe(_)

                self.ack = ack
                return ack

            def on_error(self, err):
                with lock:
                    # previous_completed = observer_completed[0]
                    # observer_completed[0] = True
                    previous_state = state[0]
                    state[0] = Completed()

                if not isinstance(previous_state, Completed):
                    observer.on_error(err)

                    self.async_upstream_ack.on_next(stop_ack)
                    self.async_upstream_ack.on_completed()

            def on_completed(self):
                """ on_next* (on_completed | on_error)?

                :return:
                """

                with lock:
                    # blindly set state to InnerTransition, analyse previous state below
                    # inner transition
                    previous_state = state[0]
                    state[0] = WaitOnNextChild(ack=self.ack)

                # print('complete inner "{}"'.format(previous_state))

                # possible previous states
                # - Active -> outer on_next call completed before this
                # - WaitOnActiveChild -> outer on_next call completes after this
                # - WaitComplete -> outer.on_complete or outer.on_error was called

                # outer on_next call hasn't completed yet, don't do anything
                if isinstance(previous_state, WaitOnActiveChild):
                    return

                # outer on_next call completed, go to next inner observable
                elif isinstance(previous_state, Active):
                    self.ack.connect_ack(self.async_upstream_ack)
                    return

                elif isinstance(previous_state, Completed):
                    with lock:
                        # todo: here something could go wrong (the short time when state is not Completed)
                        state[0] = previous_state
                    return

                elif isinstance(previous_state, WaitOuterOnCompleteOrOnError):
                    if previous_state.ex is None:
                        observer.on_completed()
                    else:
                        observer.on_error(previous_state.ex)

                    self.async_upstream_ack.on_next(stop_ack)
                    self.async_upstream_ack.on_completed()
                    return

                else:
                    raise Exception('illegal state')

        concat_map_observer = ConcatMapObserver()
        return self.source.unsafe_subscribe(concat_map_observer, scheduler, subscribe_scheduler)

