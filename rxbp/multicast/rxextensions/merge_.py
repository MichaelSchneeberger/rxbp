import threading
from typing import List

import rx
from rx import Observable
from rx.disposable import CompositeDisposable, SingleAssignmentDisposable
from rx.internal.concurrency import synchronized


def merge(sources: List[rx.typing.Observable]):
    lock = threading.RLock()

    def subscribe(observer, scheduler=None):
        group = CompositeDisposable()
        m = SingleAssignmentDisposable()
        group.add(m)

        for inner_source in sources:
            inner_subscription = SingleAssignmentDisposable()
            group.add(inner_subscription)

            @synchronized(lock)
            def on_completed(inner_subscription=inner_subscription):
                group.remove(inner_subscription)
                if len(group) == 1:
                    observer.on_completed()

            on_next = synchronized(lock)(observer.on_next)
            on_error = synchronized(lock)(observer.on_error)
            subscription = inner_source.subscribe_(on_next,
                                                   on_error,
                                                   on_completed,
                                                   scheduler)
            inner_subscription.disposable = subscription

        # group.add(Disposable(lambda: print('disposed')))
        return group

    return Observable(subscribe)


def merge_op(*others: rx.typing.Observable):
    def _(self: rx.typing.Observable):
        sources = [self] + others
        return merge(sources)

    return _
