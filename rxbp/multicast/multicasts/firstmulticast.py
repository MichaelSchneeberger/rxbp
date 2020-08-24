from dataclasses import dataclass
from typing import Callable

import rx
from rx import Observable
from rx.internal import SequenceContainsNoElementsError

from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin


@dataclass
class FirstMultiCast(MultiCastMixin):
    source: MultiCastMixin
    raise_exception: Callable[[Callable[[], None]], None]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable:
        source = self.source.get_source(info=info)

        def subscribe(observer, scheduler=None):
            def on_next(x):
                observer.on_next(x)
                observer.on_completed()

            def on_completed():
                def func():
                    raise SequenceContainsNoElementsError()

                try:
                    self.raise_exception(func)
                except Exception as exc:
                    observer.on_error(exc)

            return source.subscribe_(on_next, observer.on_error, on_completed, scheduler)

        return Observable(subscriber)
