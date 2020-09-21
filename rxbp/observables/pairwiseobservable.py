import rx

from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.pairwiseobserver import PairwiseObserver


class PairwiseObservable(Observable):
    def __init__(self, source):
        self.source = source

    def observe(self, observer_info: ObserverInfo) -> rx.typing.Disposable:
        return self.source.observe(observer_info.copy(
            observer=PairwiseObserver(
                next_observer=observer_info.observer,
            ),
        ))
