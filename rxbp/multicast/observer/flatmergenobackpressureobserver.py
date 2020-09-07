from dataclasses import dataclass
from typing import Callable, Any, List, Optional

import rx
from rx.disposable import CompositeDisposable, Disposable

from rxbp.acknowledgement.continueack import continue_ack
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observable import Observable
from rxbp.observables.mergeobservable import MergeObservable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


@dataclass
class FlatMergeNoBackpressureObserver(Observer):
    observer: Observer
    selector: Callable[[Any], Observable]
    scheduler: Scheduler
    subscribe_scheduler: Scheduler
    # observer_info: ObserverInfo
    composite_disposable: CompositeDisposable

    def __post_init__(self):
        self.place_holders = (self.PlaceHolder(observer=None), self.PlaceHolder(observer=None))

        disposable = MergeObservable(
            left=self.place_holders[0],
            right=self.place_holders[1],
        ).observe(init_observer_info(observer=self.observer))
        self.composite_disposable.add(disposable)

    @dataclass
    class PlaceHolder(Observable):
        observer: Optional[Observer]

        def observe(self, observer_info: ObserverInfo) -> rx.typing.Disposable:
            self.observer = observer_info.observer
            return Disposable()

    def on_next(self, elem: ElementType):
        obs_list: List[Observable] = [self.selector(e) for e in elem]

        for observable in obs_list:
            place_holder = self.PlaceHolder(
                observer=None,
            )

            merge_obs = MergeObservable(
                left=observable,
                right=place_holder,
            )

            parent, other = self.place_holders

            # def observe_on_subscribe_scheduler(_, __, merge_obs=merge_obs, parent=parent):
            #     return merge_obs.observe(self.observer_info.copy(observer=parent.observer))

            # # # make sure that Trampoline Scheduler is active
            # if self.subscribe_scheduler.idle:
            # disposable = self.subscribe_scheduler.schedule(observe_on_subscribe_scheduler)
            # else:
            #     disposable = observe_on_subscribe_scheduler(None, None)

            disposable = merge_obs.observe(init_observer_info(
                observer=parent.observer,
            ))
            self.composite_disposable.add(disposable)

            self.place_holders = (other, place_holder)

        return continue_ack

    def on_error(self, exc):
        def observe_on_subscribe_scheduler(_, __):
            if self.observer is not None:
                self.observer.on_error(exc)

        self.subscribe_scheduler.schedule(observe_on_subscribe_scheduler)

    def on_completed(self):
        def observe_on_subscribe_scheduler(_, __):
            for place_holder in self.place_holders:
                place_holder.observer.on_completed()

        self.subscribe_scheduler.schedule(observe_on_subscribe_scheduler)
