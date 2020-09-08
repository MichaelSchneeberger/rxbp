from dataclasses import dataclass

from rxbp.acknowledgement.acksubject import AckSubject
from rxbp.acknowledgement.continueack import ContinueAck, continue_ack
from rxbp.acknowledgement.operators.map import _map
from rxbp.acknowledgement.operators.mergeall import _merge_all
from rxbp.acknowledgement.single import Single
from rxbp.acknowledgement.stopack import StopAck, stop_ack
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


@dataclass
class ConnectableObserver(Observer):
    underlying: Observer
    # scheduler: Scheduler        # todo: remote this

    def __post_init__(self):
        self.root_ack = AckSubject()
        self.connected_ack = self.root_ack
        self.is_connected = False
        self.was_canceled = False
        self.is_volatile = False

    def connect(self):
        self.is_connected = True
        self.root_ack.on_next(continue_ack)

    def on_next(self, elem: ElementType):
        if not self.is_connected:
            def __(v):
                if isinstance(v, ContinueAck):
                    ack = self.underlying.on_next(elem)
                    return ack
                else:
                    return StopAck()

            new_ack = AckSubject()
            # _merge_all(_map(_observe_on(self.connected_ack, scheduler=self.scheduler), func=__)).subscribe(new_ack)
            _merge_all(_map(self.connected_ack, func=__)).subscribe(new_ack)

            self.connected_ack = new_ack
            return self.connected_ack
        elif not self.was_canceled:
            ack = self.underlying.on_next(elem)
            return ack
        else:
            return stop_ack

    def on_error(self, err):
        self.was_canceled = True

        if not self.is_connected:
            class ResultSingle(Single):
                def on_next(_, v):
                    if isinstance(v, ContinueAck):
                        self.underlying.on_error(err)

            # _observe_on(self.connected_ack, scheduler=self.scheduler).subscribe(ResultSingle())
            self.connected_ack.subscribe(ResultSingle())
        else:
            self.underlying.on_error(err)

    def on_completed(self):
        if not self.is_connected:
            class ResultSingle(Single):
                def on_next(_, v):
                    if isinstance(v, ContinueAck):
                        self.underlying.on_completed()

            # _observe_on(self.connected_ack, scheduler=self.scheduler).subscribe(ResultSingle())
            self.connected_ack.subscribe(ResultSingle())

        else:
            self.underlying.on_completed()
