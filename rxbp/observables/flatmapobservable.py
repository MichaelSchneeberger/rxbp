import threading
import traceback
from dataclasses import dataclass
from typing import Callable, Any, Optional

from rx.disposable import CompositeDisposable

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.single import Single
from rxbp.ack.stopack import StopAck, stop_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.observers.connectableobserver import ConnectableObserver
from rxbp.observers.flatmapobserver import FlatMapObserver
from rxbp.scheduler import Scheduler
from rxbp.states.measuredstates.flatmapstates import FlatMapStates
from rxbp.states.rawstates.rawflatmapstates import RawFlatMapStates
from rxbp.typing import ElementType


@dataclass
class FlatMapObservable(Observable):
    source: Observable
    func: Callable[[Any], Observable]
    scheduler: Scheduler
    subscribe_scheduler: Scheduler
    delay_errors: bool = False

    def observe(self, observer_info: ObserverInfo):
        composite_disposable = CompositeDisposable()

        disposable = self.source.observe(
            observer_info.copy(observer=FlatMapObserver(
                observer_info=observer_info,
                func=self.func,
                scheduler=self.scheduler,
                subscribe_scheduler=self.subscribe_scheduler,
                composite_disposable=composite_disposable,
                lock=threading.RLock(),
                state=RawFlatMapStates.InitialState(),
            )),
        )
        composite_disposable.add(disposable)

        return composite_disposable
