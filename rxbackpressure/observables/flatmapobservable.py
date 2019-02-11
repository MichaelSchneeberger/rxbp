from typing import Callable, Any, Optional

from rx import config
from rx.concurrency import CurrentThreadScheduler
from rx.core import Disposable

from rxbackpressure.ack import Ack, Continue, Stop, stop_ack
from rxbackpressure.observable import Observable
from rxbackpressure.observer import Observer


class FlatMapObservable(Observable):
    def __init__(self, source, selector: Callable[[Any], Observable], delay_errors=False):
        self.source = source
        self.selector = selector
        self.delay_errors = delay_errors

        self.lock = config['concurrency'].RLock()

    def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):

        source = self

        class State:
            pass

        class WaitOnNextChild(State):
            def __init__(self, ack):
                self.ack = ack

        class WaitOnActiveChild(State):
            pass

        class Active(State):
            def __init__(self, disposable):
                self.disposable = disposable

        class WaitComplete(State):
            def __init__(self, ex, disposable: Optional[Disposable]):
                self.ex = ex
                self.disposable = disposable

        class Cancelled(State):
            pass

        is_active = [True]
        state = [WaitOnNextChild(Continue())]

        class ChildObserver(Observer):
            def __init__(self, out: Observer, scheduler, async_upstream_ack: Ack, concat_observer):
                self.out = out
                self.scheduler = scheduler
                self.async_upstream_ack = async_upstream_ack
                self.concat_observer = concat_observer
                self.ack = Continue()

            def signal_child_on_error(self, ex):
                with source.lock:
                    current_state = state[0]
                    state[0] = WaitComplete(ex, None)

                if isinstance(current_state, WaitOnActiveChild) or isinstance(current_state, WaitOnNextChild) \
                        or isinstance(current_state, Active):
                    observer.on_error(ex)
                    self.async_upstream_ack.on_next(stop_ack)
                    self.async_upstream_ack.on_completed()
                elif isinstance(current_state, WaitComplete):
                    current_state: WaitComplete = current_state
                    scheduler.report_failure(current_state.ex)
                    observer.on_error(ex)
                    self.async_upstream_ack.on_next(stop_ack)
                    self.async_upstream_ack.on_completed()
                elif isinstance(current_state, Cancelled):
                    scheduler.report_failure(ex)
                else:
                    raise Exception('observer is in a unrecognized state: {}'.format(current_state))

            def send_on_complete(self):
                # todo: complete here
                return self.concat_observer.signal_finish()

            def signal_child_on_complete(self, ack: Ack, is_stop: bool):
                with source.lock:
                    current_state = state[0]
                    state[0] = WaitOnNextChild(ack)

                if isinstance(current_state, WaitOnNextChild) or isinstance(current_state, Active):
                    ack.subscribe(self.async_upstream_ack)
                elif isinstance(current_state, Cancelled):
                    ack.on_next(Stop())
                    ack.on_completed()
                elif isinstance(current_state, WaitComplete):
                    state_: WaitComplete = current_state
                    if not is_stop:
                        if state_.ex is None:
                            self.send_on_complete()
                        else:
                            observer.on_error(state_.ex)
                    else:
                        scheduler.report_failure(state_.ex)

            def on_stop_or_failure_ref(self, err=None):
                if err:
                    scheduler.report_failure(err)
                self.signal_child_on_complete(Stop(), is_stop=True)

            def on_next(self, v):
                ack = self.out.on_next(v)

                if isinstance(ack, Stop):
                    self.on_stop_or_failure_ref()
                elif not isinstance(v, Continue):
                    def _(v):
                        if isinstance(v, Stop):
                            self.on_stop_or_failure_ref()
                    ack.subscribe(_)

                return ack

            def on_error(self, err):
                self.signal_child_on_error(err)

            def on_completed(self):
                # self.async_upstream_ack.on_next(Continue())
                # self.async_upstream_ack.on_completed()
                self.signal_child_on_complete(self.ack, is_stop=False)

        class ConcatMapObserver(Observer):

            def __init__(self):
                self.errors = [] if source.delay_errors else None

            def cancel_state(self):
                pass

            def report_invalid_state(self, state: State, method: str):
                self.cancel_state()
                scheduler.report_failure(Exception('State {} in the ConcatMap.{} implementation is invalid'.format(state, method)))

            def on_next(self, elem):
                stream_error = True

                if not is_active[0]:
                    return stop_ack
                else:
                    async_upstream_ack = Ack()
                    child = source.selector(elem)
                    stream_error = False

                    with source.lock:
                        state[0] = WaitOnActiveChild()

                    child_observer = ChildObserver(observer, scheduler, async_upstream_ack, self)
                    disposable = child.subscribe(child_observer, scheduler, subscribe_scheduler)

                    with source.lock:
                        current_state = state[0]
                        state[0] = Active(disposable)

                    if isinstance(current_state, WaitOnNextChild):
                        with source.lock:
                            state[0] = current_state

                        state_: WaitOnNextChild = current_state
                        return state_.ack
                    elif isinstance(current_state, WaitOnActiveChild):
                        if is_active[0]:
                            return async_upstream_ack
                        else:
                            self.cancel_state()
                            return stop_ack
                    elif isinstance(current_state, Cancelled):
                        self.cancel_state()
                        return stop_ack
                    else:
                        self.report_invalid_state(current_state, 'on_next')
                        return stop_ack

            def send_on_complete(self):
                # if not source.delay_errors:
                observer.on_completed()
                # else:
                #     errors = self.errors
                #     if len(errors) == 0:
                #         observer.on_completed()
                #     else:
                #         observer.on_error(errors[0])

            def signal_finish(self, ex: Exception = None):
                current_state = state[0]
                if isinstance(current_state, Active):
                    state_: Active = current_state
                    child_ref = state_.disposable
                elif isinstance(current_state, WaitComplete):
                    state_: WaitComplete = current_state
                    child_ref = state_.disposable
                else:
                    child_ref = None

                with source.lock:
                    current_state = state[0]
                    state[0] = WaitComplete(ex, child_ref)

                if isinstance(current_state, WaitOnNextChild):
                    if ex is None:
                        self.send_on_complete()
                    else:
                        observer.on_error(ex)

                    with source.lock:
                        state[0] = Cancelled()
                elif isinstance(current_state, Active):
                    if not is_active[0]:
                        self.cancel_state()
                elif isinstance(current_state, WaitComplete):
                    with source.lock:
                        state[0] = Cancelled()
                elif isinstance(current_state, Cancelled):
                    self.cancel_state()
                    with source.lock:
                        state[0] = Cancelled()
                else:
                    self.report_invalid_state(current_state, 'signal_finish')

            def on_error(self, exc):
                self.signal_finish(exc)

            def on_completed(self):
                self.signal_finish()

        concat_map_observer = ConcatMapObserver()
        return self.source.unsafe_subscribe(concat_map_observer, scheduler, subscribe_scheduler)

