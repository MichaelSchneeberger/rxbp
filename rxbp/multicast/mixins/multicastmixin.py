import types
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic

import rx
from rx import operators as rxop
from rx.disposable import Disposable, SingleAssignmentDisposable, CompositeDisposable

import rxbp
from rxbp.flowable import Flowable
from rxbp.init.initflowable import init_flowable
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablebasemixin import FlowableBaseMixin
from rxbp.flowables.subscribeonflowable import SubscribeOnFlowable
from rxbp.multicast.flowabledict import FlowableDict
from rxbp.multicast.mixins.multicastobservablemixin import MultiCastObservableMixin
from rxbp.multicast.mixins.multicastobservermixin import MultiCastObserverMixin
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastobservables.filtermulticastobservable import FilterMultiCastObservable
from rxbp.multicast.multicastobservables.firstmulticastobservable import FirstMultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.typing import MultiCastValue
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.bufferedobserver import BufferedObserver
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType


class MultiCastMixin(Generic[MultiCastValue], ABC):
    @abstractmethod
    def unsafe_subscribe(
            self,
            subscriber: MultiCastSubscriber,
    ) -> MultiCastSubscription:
        ...

    def to_flowable(self) -> Flowable[ValueType]:
        outer_self = self

        @dataclass
        class FromMultiCastObserver(MultiCastObserverMixin):
            observer: Observer
            subscriber: Subscriber
            is_first: bool
            inner_disposable: SingleAssignmentDisposable

            def on_next(self, elem: MultiCastValue) -> None:
                self.is_first = False
                self.on_next = types.MethodType(lambda elem: None, self)  # type: ignore

                if isinstance(elem, Flowable):
                    flowable = elem
                elif isinstance(elem, list):
                    flist = [f.to_list() for f in elem if isinstance(f, Flowable)]
                    flowable = rxbp.zip(*flist)
                elif isinstance(elem, dict) or isinstance(elem, FlowableDict):
                    if isinstance(elem, dict):
                        fdict = elem
                    else:
                        fdict = elem.get_flowable_state()

                    keys, flist = zip(*((key, f.to_list()) for key, f in fdict.items() if isinstance(f, Flowable)))
                    flowable = rxbp.zip(*flist).pipe(
                        rxbp.op.map(lambda vlist: dict(zip(keys, vlist))),
                    )
                else:
                    raise Exception(f'illegal value "{elem}"')

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
            source: MultiCastObservableMixin
            subscriber: Subscriber
            multicast_subscribe_scheduler: Scheduler

            def observe(self, observer_info: ObserverInfo) -> Disposable:
                inner_disposable = SingleAssignmentDisposable()

                observer_info = MultiCastObserverInfo(FromMultiCastObserver(
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
        class FromMultiCastFlowable(FlowableBaseMixin):
            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
                multicast_subscribe_scheduler = TrampolineScheduler()

                multicast_subscription = outer_self.unsafe_subscribe(
                    subscriber=MultiCastSubscriber(
                        source_scheduler=subscriber.subscribe_scheduler,
                        multicast_scheduler=multicast_subscribe_scheduler,
                    )
                )
                source: MultiCastObservableMixin = multicast_subscription.observable

                return init_subscription(
                    observable=FromMultiCastObservable(
                        source=source,
                        subscriber=subscriber,
                        multicast_subscribe_scheduler=multicast_subscribe_scheduler,
                    ),
                )

        return init_flowable(FromMultiCastFlowable())
