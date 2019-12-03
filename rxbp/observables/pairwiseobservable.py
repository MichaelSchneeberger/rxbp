import itertools

from rxbp.ack.ackimpl import continue_ack, stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.pairwiseobserver import PairwiseObserver
from rxbp.typing import ElementType


class PairwiseObservable(Observable):
    def __init__(self, source):
        self.source = source

    def observe(self, observer_info: ObserverInfo):
        pairwise_observer = PairwiseObserver(observer_info.observer)
        pairwise_subscription = observer_info.copy(pairwise_observer)
        return self.source.observe(pairwise_subscription)
