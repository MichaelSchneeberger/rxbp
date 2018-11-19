from typing import Callable, Any

from rx import config
from rx.disposables import CompositeDisposable

from rxbackpressure.ack import Continue, Stop, Ack, stop_ack, continue_ack
from rxbackpressure.observers.anonymousobserver import AnonymousObserver
from rxbackpressure.observable import Observable


class Zip2Observable(Observable):
    def __init__(self, left, right, selector: Callable[[Any, Any], Any] = None):
        self.left = left
        self.right = right
        self.selector = (lambda l, r: (l, r)) if selector is None else selector

        self.lock = config['concurrency'].RLock()

    def unsafe_subscribe(self, observer, scheduler, subscribe_scheduler):
        is_done = [None]
        last_ack = [continue_ack]
        elem_a1 = [None]
        has_elem_a1 = [None]
        elem_a2 = [None]
        has_elem_a2 = [None]
        continue_p = [Ack()]
        complete_with_next = [False]

        def raw_on_next(a1, a2):
            if is_done[0]:
                return stop_ack
            else:
                stream_error = True

                try:
                    c = self.selector(a1, a2)
                    stream_error = False
                    ack = observer.on_next(c)
                    if complete_with_next[0]:
                        if isinstance(ack, Continue) or isinstance(ack, Stop):
                            signal_on_complete(False)
                        else:
                            ack.observe_on(scheduler).subscribe(on_completed=lambda: signal_on_complete(False))
                except Exception as ex:
                    if stream_error:
                        is_done[0] = True
                        observer.on_error(ex)
                        return stop_ack
                    else:
                        raise
                finally:
                    has_elem_a1[0] = False
                    has_elem_a2[0] = False

                return ack

        def signal_on_next(a1, a2):
            prev_last_ack = last_ack[0]

            if isinstance(prev_last_ack, Continue):
                new_last_ack = raw_on_next(a1, a2)
            elif isinstance(prev_last_ack, Stop):
                return stop_ack
            else:
                def _(v, _):
                    if isinstance(v, Continue):
                        with self.lock:
                            ack = raw_on_next(a1, a2)
                        return ack
                    else:
                        return stop_ack

                ack = Ack()
                prev_last_ack.flat_map(_).subscribe(ack)
                new_last_ack = ack

            last_ack[0] = new_last_ack

            if isinstance(new_last_ack, Continue) or isinstance(new_last_ack, Stop):
                continue_p[0].on_next(new_last_ack)
                continue_p[0].on_completed()
            else:
                new_last_ack.subscribe(continue_p[0])

            # acknowledgment used by input that receives first
            continue_p[0] = Ack()

            return new_last_ack

        def signal_on_error(ex):
            with self.lock:
                if not is_done[0]:
                    is_done[0] = True
                    observer.on_error(ex)
                    last_ack[0] = stop_ack

        def signal_on_complete(has_elem):
            def raw_on_completed():
                if not is_done[0]:
                    is_done[0] = True
                    observer.on_completed()

            with self.lock:
                if not has_elem:
                    if isinstance(last_ack[0], Continue):
                        raw_on_completed()
                    elif isinstance(last_ack[0], Stop):
                        pass
                    else:
                        def _(v):
                            if isinstance(v, Continue):
                                with self.lock:
                                    raw_on_completed()
                            else:
                                pass

                        last_ack[0].observe_on(scheduler).subscribe(on_next=_)


                    continue_p[0].on_next(stop_ack)
                    last_ack[0] = stop_ack
                else:
                    complete_with_next[0] = True

        def on_next_left(elem):
            with self.lock:
                if is_done[0]:
                    return_ack = stop_ack
                else:
                    elem_a1[0] = elem
                    if not has_elem_a1[0]:
                        has_elem_a1[0] = True

                    if has_elem_a2[0]:
                        return_ack = signal_on_next(elem_a1[0], elem_a2[0])
                    else:
                        return_ack = continue_p[0]
                return return_ack

        def on_next_right(elem):
            with self.lock:
                if is_done[0]:
                    return_ack = stop_ack
                else:
                    elem_a2[0] = elem
                    if not has_elem_a2[0]:
                        has_elem_a2[0] = True

                    if has_elem_a1[0]:
                        return_ack = signal_on_next(elem_a1[0], elem_a2[0])
                    else:
                        return_ack = continue_p[0]
                return return_ack

        def on_error(ex):
            signal_on_error(ex)

        def on_completed_left():
            return signal_on_complete(has_elem_a1[0])

        def on_completed_right():
            return signal_on_complete(has_elem_a2[0])

        left_observer = AnonymousObserver(on_next=on_next_left, on_error=on_error,
                                          on_completed=on_completed_left)
        d1 = self.left.unsafe_subscribe(left_observer, scheduler, subscribe_scheduler)

        right_observer = AnonymousObserver(on_next=on_next_right, on_error=on_error,
                                           on_completed=on_completed_right)
        d2 = self.right.unsafe_subscribe(right_observer, scheduler, subscribe_scheduler)

        return CompositeDisposable(d1, d2)
