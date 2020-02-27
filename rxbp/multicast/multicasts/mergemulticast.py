from typing import Iterator

import rx

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.rxextensions.merge_ import merge


class MergeMultiCast(MultiCastBase):
    def __init__(
            self,
            sources: Iterator[MultiCastBase],
    ):
        self.sources = sources

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:

        sources = [e.get_source(info=info) for e in self.sources]

        return merge(sources)

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