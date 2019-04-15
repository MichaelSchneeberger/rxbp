from typing import Callable, Any, Set

from rx.core import Disposable

from rxbp.observable import Observable
from rxbp.subscriber import Subscriber
from rxbp.subscriptable import Subscriptable
from rxbp.subscriptablebase import SubscriptableBase


class AnonymousSubscriptable(Subscriptable):
    def __init__(self,
                 unsafe_subscribe_func: Callable[[Subscriber], Subscriptable.SubscribeReturnType],
                 base: Any = None,
                 selectable_bases: Set[Any] = None):

        class InnerSubscriptable(SubscriptableBase):
            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscriptable.SubscribeReturnType:
                return unsafe_subscribe_func(subscriber)

        subscriptable_base = InnerSubscriptable(base=base, selectable_bases=selectable_bases)

        super().__init__(subscriptable=subscriptable_base)
