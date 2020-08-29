import types
from dataclasses import dataclass

from rx.disposable import SingleAssignmentDisposable

import rxbp
from rxbp.flowable import Flowable
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.flowabledict import FlowableDict
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.typing import MultiCastItem
from rxbp.observer import Observer
from rxbp.subscriber import Subscriber


@dataclass
class ToFlowableMultiCastObserver(MultiCastObserver):
    observer: Observer
    subscriber: Subscriber
    is_first: bool
    inner_disposable: SingleAssignmentDisposable

    def on_next(self, elem: MultiCastItem) -> None:
        if isinstance(elem, list):
            first_elem = elem[0]

        else:
            try:
                first_elem = next(elem)
            except StopIteration:
                return

        self.is_first = False
        self.on_next = types.MethodType(lambda self, elem: None, self)  # type: ignore

        if isinstance(first_elem, FlowableMixin):
            flowable = first_elem
        elif isinstance(first_elem, list):
            flist = [f.to_list() for f in first_elem if isinstance(f, Flowable)]
            flowable = rxbp.zip(*flist)
        elif isinstance(first_elem, dict) or isinstance(first_elem, FlowableDict):
            if isinstance(first_elem, dict):
                fdict = first_elem
            else:
                fdict = first_elem.get_flowable_state()

            keys, flist = zip(*((key, f.to_list()) for key, f in fdict.items() if isinstance(f, FlowableMixin)))
            flowable = rxbp.zip(*flist).pipe(
                rxbp.op.map(lambda vlist: dict(zip(keys, vlist))),
            )
        else:
            raise Exception(f'illegal value "{first_elem}"')

        subscription = flowable.unsafe_subscribe(subscriber=self.subscriber)

        def subscribe_action(_, __):
            return subscription.observable.observe(init_observer_info(
                observer=self.observer,
            ))

        self.inner_disposable.disposable = self.subscriber.subscribe_scheduler.schedule(subscribe_action)

    def on_error(self, exc: Exception) -> None:
        self.observer.on_error(exc)

    def on_completed(self) -> None:
        if self.is_first:
            self.observer.on_completed()
