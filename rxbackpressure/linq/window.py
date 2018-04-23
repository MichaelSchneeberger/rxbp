from typing import Callable, Any

from rx import config
from rx.concurrency import current_thread_scheduler
from rx.core import Disposable
from rx.disposables import CompositeDisposable, MultipleAssignmentDisposable, RefCountDisposable, \
    SingleAssignmentDisposable
from rx.internal import extensionmethod
from rx.subjects import AsyncSubject

from rxbackpressure.backpressuretypes.controlledbackpressure import ControlledBackpressure
from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.backpressuretypes.windowbackpressure import WindowBackpressure
from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.anonymoussubflowobservable import AnonymousSubFlowObservable
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.subjects.subflowsyncedsubject import SubFlowSyncedSubject
from rxbackpressure.subjects.syncedsubject import SyncedSubject


@extensionmethod(BackpressureObservable)
def window(self,
           other: BackpressureObservable,
           is_lower: Callable[[Any, Any], bool],
           is_higher: Callable [[Any, Any], bool]) -> BackpressureObservable:
    """ For each element of the backpressured observable sequence create
    a window with elements of the other backpressured observable that are
    neither lower nor higher.

    Example:
        t1 = Observable.range(1, 10).map(lambda v, i: v*0.1).pairwise().to_backpressure()
        t2 = Observable.range(1, 100).map(lambda v, i: v*0.03-0.01).to_backpressure()

        t1.window(t2, lambda v1, v2: v2 < v1[0], lambda v1, v2: v1[1] <= v2) \
            .to_observable().subscribe(lambda v: v[1].to_observable().to_list().subscribe(print))

    :param other: backpressued observable
    :param is_lower: ignore elements from other as long as they are lower than the current element from self
    :param is_higher: create empty window as long as the current element from other is higher than elements from self
    :returns: A backpressure observable emitting tuples (element from self, synced backpressure subject)
    """

    source = self

    def subscribe_func(observer, parent_scheduler):
        lock = config["concurrency"].RLock()
        element_list = []
        opening_list = []
        parent_scheduler = parent_scheduler or current_thread_scheduler
        current_subject = [None]
        backpressure = [None]
        element_backpressure = [None]
        to_be_buffered = [0]
        is_running = [False]
        is_stopped = [False]
        is_stopped2 = [False]
        multiple_assignment_disposable = MultipleAssignmentDisposable()
        single_assignment_disposable = SingleAssignmentDisposable()
        composite_disposable = CompositeDisposable(single_assignment_disposable, multiple_assignment_disposable)
        ref_count_disposable = RefCountDisposable(composite_disposable)

        def start_process():
            # print('start process actually started')
            def action(a, s):
                # print('process {}'.format(element_list))
                if is_stopped2[0] == True:
                    element = None
                else:
                    element = element_list[0]
                opening = opening_list[0]

                if is_stopped2[0] == True or is_higher(opening, element):
                    # print('is higher')
                    if is_stopped[0] == True and is_stopped2[0] == False:
                        element_backpressure[0].request(StopRequest())
                        with self.lock:
                            is_running[0] = False
                            return

                    if current_subject[0] is not None:
                        # complete subject
                        current_subject[0].on_completed()
                        current_subject[0] = None

                    # remove first opening
                    opening_list.pop(0)

                    num = element_backpressure[0].finish_current_request()
                    to_be_buffered[0] += num

                    backpressure[0].update()
                elif is_lower(opening, element):
                    # remove first element
                    element_list.pop(0)

                    element_backpressure[0].remove_element_and_update()
                else:
                    if current_subject[0] is None:
                        send_new_subject(value = opening)

                    # send element to inner subject
                    current_subject[0].on_next(element)

                    # remove first element
                    element_list.pop(0)

                    element_backpressure[0].next_element()

                with lock:
                    if len(opening_list) > 0 and len(element_list) > 0:
                        # print('start process again')
                        start_process()
                    else:
                        is_running[0] = False

            # print('schedule start process')
            parent_scheduler.schedule(action)

        def send_new_subject(value):
            synced_subject = SyncedSubject(scheduler=parent_scheduler)
            current_subject[0] = synced_subject
            observer.on_next((value, synced_subject))
            disposable = synced_subject.subscribe_backpressure(element_backpressure[0], parent_scheduler)

            # todo: what to do with disposable?
            multiple_assignment_disposable.disposable = disposable

        def on_next_opening(value):
            # print('opening received value={}'.format(value))

            with lock:
                if current_subject[0] is None:
                    send_new_subject(value)

                if is_stopped2[0] == True or (len(opening_list) == 0 and len(element_list) > to_be_buffered[0]):
                    opening_list.append(value)
                    if not is_running[0]:
                        is_running[0] = True
                        start_process()
                else:
                    opening_list.append(value)

        def on_next_element(value):
            # print('element received value {}'.format(value))

            with lock:
                element_list.append(value)
                if len(element_list) == 1 + to_be_buffered[0] and len(opening_list) > 0:
                    if not is_running[0]:
                        is_running[0] = True
                        start_process()

        def subscribe_pb_opening(parent_backpressure, scheduler=None):
            backpressure[0] = ControlledBackpressure(parent_backpressure, scheduler=parent_scheduler)
            disposable = observer.subscribe_backpressure(backpressure[0])
            single_assignment_disposable.disposable = disposable

            # only forward disposable
            # return CompositeDisposable(disposable, multiple_assignment_disposable)
            return ref_count_disposable.disposable

        def subscribe_pb_element(backpressure, scheduler=None):
            def request_from_buffer(num):
                with lock:
                    to_be_buffered[0] -= num
                    # print('request from buffer {}'.format(to_be_buffered[0]))
                    if len(element_list) == 1 + to_be_buffered[0] and len(opening_list) > 0:
                        if not is_running[0]:
                            is_running[0] = True
                            # print('start process')
                            start_process()

            element_backpressure[0] = WindowBackpressure(backpressure, request_from_buffer=request_from_buffer,
                                                         scheduler=parent_scheduler)
            # return Disposable.empty()
            return ref_count_disposable.disposable

        def on_completed(idx):
            """ Complete observer if window has not been stopped yet
            """
            # print('oncompleted {}'.format(idx))
            complete = False
            start_process2 = False
            stop_request = False
            with lock:
                if idx==1 and is_stopped[0] == False:
                    # print('complete1')
                    is_stopped[0] = True
                    if is_stopped2[0] == True:
                        complete = True
                    else:
                        stop_request = True
                elif idx==2 and is_stopped2[0] == False:
                    is_stopped2[0] = True
                    if is_stopped[0] == True:
                        complete = True
                    else:
                        start_process2 = True
            if complete:
                observer.on_completed()

            if start_process2:
                start_process()

            if stop_request:
                element_backpressure[0].request(StopRequest())
                # if current_subject[0]:
                #     current_subject[0].on_completed()
                # backpressure[0].request(StopRequest())
                # element_backpressure[0].request(StopRequest())

        d1 = other.subscribe(on_next=on_next_element, on_completed=lambda: on_completed(2), on_error=observer.on_error,
                        subscribe_bp=subscribe_pb_element)
        d2 = source.subscribe(on_next=on_next_opening, on_completed=lambda: on_completed(1), on_error=observer.on_error,
                         subscribe_bp=subscribe_pb_opening)
        return CompositeDisposable(d1, d2)

    obs = AnonymousSubFlowObservable(subscribe_func=subscribe_func, name='window')
    return obs
