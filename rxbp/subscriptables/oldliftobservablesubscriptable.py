from typing import Callable, Any, Dict

from rx.core import Disposable

from rxbp.internal.indexingop import index_observable
from rxbp.observable import Observable
from rxbp.observables.filterobservable import FilterObservable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.subscriber import Subscriber
from rxbp.subscribers.anonymoussubscriber import AnonymousSubscriber
from rxbp.subscriptablebase import SubscriptableBase


class LiftObservableSubscriptable(SubscriptableBase):
    def __init__(self, source: SubscriptableBase,
                 func: Callable[[Subscriber, Observable, Dict[Any, Observable]], Disposable], base: Any = None):
        super().__init__(base=base or self)

        self.source = source
        self.func = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> Disposable:
        def on_subscribe_func(source: Observable, selectors: Dict[Any, Observable]):
            # return subscriber.on_subscribe(observable=self.func(source), selectors=selectors)
            return self.func(subscriber, source, selectors)

        subscriber_ = AnonymousSubscriber(on_subscribe_func=on_subscribe_func, scheduler=subscriber.scheduler,
                            subscribe_scheduler=subscriber.subscribe_scheduler)

        return self.source.unsafe_subscribe(subscriber=subscriber_)


class Lift2ObservableSubscriptable(SubscriptableBase):
    def __init__(self, left: SubscriptableBase, right: SubscriptableBase,
                 func: Callable[[Subscriber, Observable, Observable, Dict[Any, Observable]], Disposable], ignore_mismatch: bool = None):
        super().__init__(base=left.base)

        # if ignore_mismatch is not True:
        #
        #     if left.base == right.base:
        #         self.transform_left = False
        #         self.transform_right = False
        #     elif right.transformables is not None and left.base in right.transformables:
        #         self.transform_left = True
        #         self.transform_right = False
        #     elif left.transformables is not None and right.base in left.transformables:
        #         self.transform_left = False
        #         self.transform_right = True
        #     else:
        #         raise AssertionError('flowable do not match')
        #
        # else:
        #     self.transform_left = False
        #     self.transform_right = False

        self.ignore_mismatch = ignore_mismatch
        self.left = left
        self.right = right
        self.func = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> Disposable:
        def on_left_subscribe_func(left_obs: Observable, left_selectors: Dict[Any, Observable]):
            def on_right_subscribe_func(right_obs: Observable, right_selectors: Dict[Any, Observable]):

                if self.ignore_mismatch is not True:
                    if self.left.base == self.right.base:
                        self.transform_left = False
                        self.transform_right = False
                    elif left_selectors is not None and self.left.base in right_selectors:
                        self.transform_left = True
                        self.transform_right = False
                    elif left_selectors is not None and self.right.base in left_selectors:
                        self.transform_left = False
                        self.transform_right = True
                    else:
                        raise AssertionError('flowable do not match')

                else:
                    self.transform_left = False
                    self.transform_right = False

                selectors = {}

                if self.transform_left:
                    index_obs = right_selectors[self.left.base]
                    zip_obs = Zip2Observable(left=left_obs, right=index_obs)
                    filter_obs = FilterObservable(source=zip_obs, predicate=lambda v: v[1][0], scheduler=subscriber.scheduler)
                    left_obs_ = MapObservable(source=filter_obs, selector=lambda v: v[0])
                else:
                    selectors = {**selectors, **left_selectors}
                    left_obs_ = left_obs
                    
                if self.transform_right:
                    index_obs = left_selectors[self.right.base]
                    # zip_obs = Zip2Observable(left=right_obs, right=index_obs)
                    # filter_obs = FilterObservable(source=zip_obs, predicate=lambda v: v[1][0], scheduler=subscriber.scheduler)
                    # right_obs_ = MapObservable(source=filter_obs, selector=lambda v: v[0])
                    right_obs_ = index_observable(right_obs, index_obs)
                else:
                    selectors = {**selectors, **right_selectors}
                    right_obs_ = right_obs

                disposable = self.func(subscriber, left_obs_, right_obs_, selectors)
                return disposable

            right_subscriber = AnonymousSubscriber(on_subscribe_func=on_right_subscribe_func, scheduler=subscriber.scheduler,
                                              subscribe_scheduler=subscriber.subscribe_scheduler)

            return self.right.unsafe_subscribe(subscriber=right_subscriber)

        left_subscriber = AnonymousSubscriber(on_subscribe_func=on_left_subscribe_func, scheduler=subscriber.scheduler,
                            subscribe_scheduler=subscriber.subscribe_scheduler)

        return self.left.unsafe_subscribe(subscriber=left_subscriber)

