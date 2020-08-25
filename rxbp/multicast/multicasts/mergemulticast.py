from dataclasses import dataclass
from typing import Iterable

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.mergemulticastobservable import MergeMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription


@dataclass
class MergeMultiCast(MultiCastMixin):
    sources: Iterable[MultiCastMixin]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        subscriptions = [source.unsafe_subscribe(subscriber=subscriber) for source in self.sources]

        return subscriptions[0].copy(
            observable=MergeMultiCastObservable(
                sources=[s.observable for s in subscriptions],
                scheduler=subscriber.multicast_scheduler,
            ),
        )

        # lock = threading.RLock()
        #
        # def subscribe(observer, scheduler=None):
        #     group = CompositeDisposable()
        #     m = SingleAssignmentDisposable()
        #     group.add(m)
        #
        #     for inner_source in sources:
        #         inner_subscription = SingleAssignmentDisposable()
        #         group.add(inner_subscription)
        #
        #         # inner_source = rx.from_future(inner_source) if is_future(
        #         #     inner_source) else inner_source
        #
        #         @synchronized(lock)
        #         def on_completed(inner_subscription=inner_subscription):
        #             group.remove(inner_subscription)
        #             if len(group) == 1:
        #                 observer.on_completed()
        #
        #         on_next = synchronized(lock)(observer.on_next)
        #         on_error = synchronized(lock)(observer.on_error)
        #         subscription = inner_source.subscribe_(on_next,
        #                                                on_error,
        #                                                on_completed,
        #                                                scheduler)
        #         inner_subscription.disposable = subscription
        #
        #     # group.add(Disposable(lambda: print('disposed')))
        #     return group
        #
        # return Observable(subscribe)