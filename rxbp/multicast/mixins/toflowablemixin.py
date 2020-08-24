import types
from abc import ABC
from dataclasses import dataclass

from rx.disposable import Disposable, SingleAssignmentDisposable, CompositeDisposable

import rxbp
from rxbp.flowable import Flowable
from rxbp.init.initflowable import init_flowable
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.flowabledict import FlowableDict
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.typing import MultiCastItem
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType


class ToFlowableMixin(ABC):
    def to_flowable(self) -> Flowable[ValueType]:
        outer_self = self

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

        @dataclass
        class FromMultiCastObservable(Observable):
            source: MultiCastObservable
            subscriber: Subscriber
            multicast_subscribe_scheduler: Scheduler

            def observe(self, observer_info: ObserverInfo) -> Disposable:
                inner_disposable = SingleAssignmentDisposable()

                observer_info = MultiCastObserverInfo(ToFlowableMultiCastObserver(
                    observer=observer_info.observer,
                    subscriber=self.subscriber,
                    is_first=True,
                    inner_disposable=inner_disposable,
                ))

                def subscribe_action(_, __):
                    return CompositeDisposable(
                        self.source.observe(observer_info),
                        inner_disposable,
                    )

                return self.multicast_subscribe_scheduler.schedule(subscribe_action)

        @dataclass
        class FromMultiCastFlowable(FlowableMixin):
            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
                multicast_subscribe_scheduler = TrampolineScheduler()

                multicast_subscription = outer_self.unsafe_subscribe(
                    subscriber=MultiCastSubscriber(
                        source_scheduler=subscriber.subscribe_scheduler,
                        multicast_scheduler=multicast_subscribe_scheduler,
                    )
                )
                source: MultiCastObservable = multicast_subscription.observable

                return init_subscription(
                    observable=FromMultiCastObservable(
                        source=source,
                        subscriber=subscriber,
                        multicast_subscribe_scheduler=multicast_subscribe_scheduler,
                    ),
                )

        return init_flowable(FromMultiCastFlowable())
