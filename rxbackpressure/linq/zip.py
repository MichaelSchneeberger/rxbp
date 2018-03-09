from rx import AnonymousObservable
from rx.core import Disposable
from rx.disposables import CompositeDisposable
from rx.internal import extensionmethod

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable


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

        def subscribe_bp(scheduler):
            parent_backpressure = ZipBackpressure(backpressure1[0], backpressure2[0])
            disposable = observer.subscribe_backpressure(parent_backpressure, scheduler)

            return CompositeDisposable(disposable, Disposable.create(parent_backpressure.dispose))

        def subscribe_bp_1(backpressure, scheduler):
            # print('subscribe backpressure 1')
            backpressure1[0] = backpressure
            count[0] += 1

            if backpressure2[0] is not None:
                connectable_subscription[0] = subscribe_bp(scheduler)

            def dispose():
                count[0] -= 1
                if count[0] == 0:
                    connectable_subscription[0].dispose()

            return Disposable.create(dispose)

        def subscribe_bp_2(backpressure, scheduler):
            # print('subscribe backpressure 2')
            backpressure2[0] = backpressure
            count[0] += 1

            if backpressure1[0] is not None:
                connectable_subscription[0] = subscribe_bp(scheduler)

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
        disposable = obs1.zip(obs2, selector).subscribe(observer)
        # print(disposable)
        return disposable

    obs = AnonymousBackpressureObservable(subscribe_func=subscribe_func)
    return obs
