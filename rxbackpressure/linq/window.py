from typing import Callable, Any

from rx import config
from rx.concurrency import current_thread_scheduler
from rx.disposables import CompositeDisposable
from rx.internal import extensionmethod

from rxbackpressure.backpressuretypes.controlledbackpressure import ControlledBackpressure
from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.backpressuretypes.windowbackpressure import WindowBackpressure
from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.internal.blockingfuture import BlockingFuture
from rxbackpressure.subjects.syncedbackpressuresubject import SyncedBackpressureSubject


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

    def subscribe_func(observer):
        lock = config["concurrency"].RLock()
        element_list = []
        opening_list = []
        scheduler = current_thread_scheduler
        current_subject = [None]
        backpressure = [None]
        element_backpressure = [None]

        def start_process():
            def action(a, s):
                element = element_list[0]
                opening = opening_list[0]

                if is_lower(opening, element):
                    # remove first element
                    element_list.pop(0)

                    element_backpressure[0].remove_element()
                elif is_higher(opening, element):
                    # complete subject
                    current_subject[0].on_completed()
                    current_subject[0] = None
                    # remove first opening
                    opening_list.pop(0)

                    element_backpressure[0].finish_current_request()
                    backpressure[0].update()
                else:
                    # send element to inner subject
                    current_subject[0].on_next(element)

                    # remove first element
                    element_list.pop(0)

                    element_backpressure[0].next_element()

                with lock:
                    if len(opening_list) > 0 and len(element_list) > 0:
                        start_process()

            scheduler.schedule(action)

        def send_new_subject(value):
            current_subject[0] = SyncedBackpressureSubject()
            current_subject[0].subscribe_backpressure(element_backpressure[0])
            observer.on_next((value, current_subject[0]))

        def on_next_opening(value):
            # print('opening received value={}'.format(value))

            with lock:
                if current_subject[0] is None:
                    send_new_subject(value)

                if len(opening_list) == 0 and len(element_list) > 0:
                    opening_list.append(value)
                    start_process()
                else:
                    opening_list.append(value)

        def on_next_element(value):
            # print('element received value {}'.format(value))
            with lock:
                if len(element_list) == 0 and len(opening_list) > 0:
                    element_list.append(value)
                    start_process()
                else:
                    # len(element_list) > 0, then the process has already been started
                    element_list.append(value)

        def subscribe_pb_opening(parent_backpressure):
            backpressure[0] = ControlledBackpressure(parent_backpressure)
            observer.subscribe_backpressure(backpressure[0])
            # observer.subscribe_backpressure(parent_backpressure)
            # return backpressure[0]

        def subscribe_pb_element(backpressure):
            element_backpressure[0] = WindowBackpressure(backpressure)

        def on_completed():
            with lock:
                if current_subject[0]:
                    current_subject[0].on_completed()
            observer.on_completed()

        d1 = other.subscribe(on_next=on_next_element, on_completed=on_completed, on_error=observer.on_error,
                        subscribe_bp=subscribe_pb_element)
        d2 = source.subscribe(on_next=on_next_opening, on_completed=on_completed, on_error=observer.on_error,
                         subscribe_bp=subscribe_pb_opening)
        return CompositeDisposable(d1, d2)

    obs = AnonymousBackpressureObservable(subscribe_func=subscribe_func)
    return obs
