from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.pairwiseobserver import PairwiseObserver


class PairwiseObservable(Observable):
    def __init__(self, source):
        self.source = source

    def observe(self, observer_info: ObserverInfo):
        pairwise_observer = PairwiseObserver(observer_info.observer)
        pairwise_subscription = observer_info.copy(pairwise_observer)
        return self.source.observe(pairwise_subscription)
