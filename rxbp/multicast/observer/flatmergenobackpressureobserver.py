from typing import Callable, Any, List, Optional

from rx.core.typing import Disposable
from rx.disposable import CompositeDisposable

from rxbp.ack.continueack import continue_ack
from rxbp.observable import Observable
from rxbp.observables.mergeobservable import MergeObservable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


class FlatMergeNoBackpressureObserver(Observer):
    def __init__(
            self,
            observer: Observer,
            selector: Callable[[Any], Observable],
            scheduler: Scheduler,
            subscribe_scheduler: Scheduler,
            observer_info: ObserverInfo,
            composite_disposable: CompositeDisposable,
    ):
        self.observer = observer
        self.selector = selector
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler
        self.observer_info = observer_info
        self.composite_disposable = composite_disposable

        self.place_holders = (self.PlaceHolder(observer=None), self.PlaceHolder(observer=None))

        disposable = MergeObservable(
            left=self.place_holders[0],
            right=self.place_holders[1],
        ).observe(self.observer_info.copy(observer=observer))
        composite_disposable.add(disposable)

    # @dataclass
    class PlaceHolder(Observable):
        # observer: Optional[Observer]
        def __init__(self, observer: Optional[Observer]):
            self.observer = None

        def observe(self, observer_info: ObserverInfo) -> Disposable:
            self.observer = observer_info.observer

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

            def observe_on_subscribe_scheduler(_, __, merge_obs=merge_obs, parent=parent):
                return merge_obs.observe(self.observer_info.copy(observer=parent.sink))

            # make sure that Trampoline Scheduler is active
            disposable = self.subscribe_scheduler.schedule(observe_on_subscribe_scheduler)
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
