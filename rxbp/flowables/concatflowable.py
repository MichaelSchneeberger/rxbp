from typing import Callable, Any, Iterator, Iterable

from rxbp.flowablebase import FlowableBase
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.selectors.bases import NumericalBase, ConcatBase
from rxbp.selectors.selectionop import merge_selectors
from rxbp.observables.filterobservable import FilterObservable
from rxbp.subscriber import Subscriber


class ConcatFlowable(FlowableBase):
    def __init__(self, sources: Iterable[FlowableBase]):
        sources = list(sources)

        # the base becomes anonymous after concatenating
        if all(isinstance(source.base, NumericalBase) for source in sources):
            base = sum(source.base.num for source in sources)
        else:
            base = ConcatBase(source.base for source in sources)

        super().__init__(base=base)

        self._sources = sources

    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        source_observables, _ = zip(*[source.unsafe_subscribe(subscriber) for source in self._sources])

        observable = ConcatObservable(sources=source_observables, subscribe_scheduler=subscriber.subscribe_scheduler)
        return observable, {}
