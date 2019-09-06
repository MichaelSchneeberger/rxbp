from typing import Callable, Any, Iterator, Iterable

from rxbp.flowablebase import FlowableBase
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.selectors.bases import NumericalBase, Base
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class ConcatFlowable(FlowableBase):
    def __init__(self, sources: Iterable[Base]):
        sources = list(sources)

        # the base becomes anonymous after concatenating
        # if all(isinstance(source.base, NumericalBase) for source in sources):
        #     base = NumericalBase(sum(source.base.num for source in sources))
        # else:
        underlying = list(source.base for source in sources)
        all_bases = all(isinstance(base, Base) for base in underlying)
        if all_bases:
            # base = ConcatBase(underlying=underlying)
            raise NotImplementedError
        else:
            base = None

        super().__init__(base=base)

        self._sources = sources

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        source_observables, _ = zip(*[source.unsafe_subscribe(subscriber) for source in self._sources])

        observable = ConcatObservable(sources=source_observables, subscribe_scheduler=subscriber.subscribe_scheduler)
        return observable, {}
