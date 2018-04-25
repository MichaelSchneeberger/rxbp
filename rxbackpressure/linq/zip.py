from rx import AnonymousObservable, Observable
from rx.core import Disposable
from rx.disposables import CompositeDisposable, SingleAssignmentDisposable
from rx.internal import extensionmethod

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable


def _zip(*args):
    """Merges the specified observable sequences into one observable
    sequence by using the selector function whenever all of the observable
    sequences or an array have produced an element at a corresponding index.
    The last element in the arguments must be a function to invoke for each
    series of elements at corresponding indexes in the sources.
    1 - res = obs1.zip(obs2, fn)
    2 - res = x1.zip([1,2,3], fn)
    Returns an observable sequence containing the result of combining
    elements of the sources using the specified result selector function.
    """

    sources = list(args)
    result_selector = sources.pop()

    def subscribe(observer):
        n = len(sources)
        queues = [[] for _ in range(n)]
        is_done = [False] * n

        def next(i):
            if all([len(q) for q in queues]):
                try:
                    queued_values = [x.pop(0) for x in queues]
                    res = result_selector(*queued_values)
                except Exception as ex:
                    observer.on_error(ex)
                    return

                observer.on_next(res)
            elif all([x for j, x in enumerate(is_done) if j != i]):
                observer.on_completed()

        def done(i):
            # is_done[i] = True
            # if all(is_done):
            observer.on_completed()

        subscriptions = [None]*n

        def func(i):
            source = sources[i]
            sad = SingleAssignmentDisposable()
            source = Observable.from_future(source)

            def on_next(x):
                queues[i].append(x)
                next(i)

            sad.disposable = source.subscribe(on_next, observer.on_error, lambda: done(i))
            subscriptions[i] = sad
        for idx in range(n):
            func(idx)
        return CompositeDisposable(subscriptions)
    return AnonymousObservable(subscribe)


@extensionmethod(BackpressureObservable)
def zip(self, other, selector=lambda v1, v2: (v1, v2)):
    class ZipBackpressure(BackpressureBase):
        def __init__(self, backpressure1, backpressure2):
            # print('create zip backpressure')
            self.backpressure1 = backpressure1
            self.backpressure2 = backpressure2
            self.is_stopped = False

        def request(self, number_of_items):
            if not self.is_stopped:
                # print('zip request %s' % number_of_items)
                f1 = self.backpressure1.request(number_of_items)
                f2 = self.backpressure2.request(number_of_items)

                def selector(val1, val2):
                    # print('selector {} {}'.format(val1, val2))
                    if val1 < number_of_items and val2 == number_of_items:
                        self.backpressure2.request(StopRequest())
                    elif val1 == number_of_items and val2 < number_of_items:
                        self.backpressure1.request(StopRequest())

                    return min(val1, val2)

                return f1.zip(f2, selector)

        def dispose(self):
            self.is_stopped = True
            # todo: is this necessary?
            self.backpressure1 = None
            self.backpressure2 = None

    def subscribe_func(observer, scheduler):
        backpressure1 = [None]
        backpressure2 = [None]
        connectable_subscription = [None]
        count = [0]
        parent_scheduler = scheduler

        def subscribe_bp():
            parent_backpressure = ZipBackpressure(backpressure1[0], backpressure2[0])
            disposable = observer.subscribe_backpressure(parent_backpressure)

            return CompositeDisposable(disposable, Disposable.create(parent_backpressure.dispose))

        def subscribe_bp_1(backpressure):
            # print('subscribe backpressure 1')
            backpressure1[0] = backpressure
            count[0] += 1

            if backpressure2[0] is not None:
                connectable_subscription[0] = subscribe_bp()

            def dispose():
                count[0] -= 1
                if count[0] == 0:
                    connectable_subscription[0].dispose()

            return Disposable.create(dispose)

        def subscribe_bp_2(backpressure):
            # print('subscribe backpressure 2')
            backpressure2[0] = backpressure
            count[0] += 1

            if backpressure1[0] is not None:
                connectable_subscription[0] = subscribe_bp()

            def dispose():
                count[0] -= 1
                if count[0] == 0:
                    connectable_subscription[0].dispose()


            return Disposable.create(dispose)

        obs1 = AnonymousObservable(subscribe=lambda observer: self.subscribe(observer=observer,
                                                                             subscribe_bp=subscribe_bp_1,
                                                                             scheduler=scheduler))
        obs2 = AnonymousObservable(subscribe=lambda observer: other.subscribe(observer=observer,
                                                                              subscribe_bp=subscribe_bp_2))

        def on_completed():
            stop_request = StopRequest()
            backpressure1[0].request(stop_request)
            backpressure2[0].request(stop_request)

        disposable = _zip(obs1, obs2, selector).do_action(on_completed=on_completed).subscribe(observer)
        # print(disposable)
        return disposable

    obs = AnonymousBackpressureObservable(subscribe_func=subscribe_func, name='zip')
    return obs
