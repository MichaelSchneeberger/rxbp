from typing import Iterable

from rx.core.typing import Disposable
from rxbp.ack.ackbase import AckBase
from rxbp.ack.merge import _merge
from rxbp.flowablebase import FlowableBase
from rxbp.observable import Observable
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.observablesubjects.osubjectbase import OSubjectBase
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.bases import ConcatBase
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo
from rxbp.typing import ElementType


class ConcatFlowable(FlowableBase):
    def __init__(self, sources: Iterable[FlowableBase]):

        super().__init__()

        self._sources = list(sources)

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        def gen_subscriptions():
            for source in self._sources:
                subscription = source.unsafe_subscribe(subscriber)

                selector = PublishOSubject(scheduler=subscriber.scheduler)

                class ObservableWithPassivListener(Observable):
                    def __init__(self, observable: Observable, selector: OSubjectBase):
                        self.observable = observable
                        self.selector = selector

                    def observe(self, observer_info: ObserverInfo) -> Disposable:
                        observer = observer_info.observer
                        source = self

                        class ObserverWithPassivListener(Observer):

                            def on_next(self, elem: ElementType) -> AckBase:
                                ack1 = source.selector.on_next(elem)
                                ack2 = observer.on_next(elem)
                                return _merge(ack1, ack2)

                            def on_error(self, exc: Exception):
                                source.selector.on_error(exc)
                                observer.on_error(exc)

                            def on_completed(self):
                                source.selector.on_completed()
                                observer.on_completed()

                        observer_info = observer_info.copy(observer=ObserverWithPassivListener())
                        return self.observable.observe(observer_info)

                # subject = ObservableCacheServeFirstSubject(scheduler=subscriber.scheduler)
                # observable = RefCountObservable(source=subscription.observable, subject=subject)

                observable = ObservableWithPassivListener(subscription.observable, selector)

                # yield subscription.info, subscription.observable
                yield subscription.info, observable, selector

        infos, sources, selectors = zip(*gen_subscriptions())

        observable = ConcatObservable(
            sources=sources,
            scheduler=subscriber.scheduler,
            subscribe_scheduler=subscriber.subscribe_scheduler,
        )

        return Subscription(info=SubscriptionInfo(ConcatBase(infos, selectors)), observable=observable)
        # return Subscription(info=SubscriptionInfo(base=None), observable=observable)
