from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.ack.operators.map import _map
from rxbp.ack.operators.mergeall import _merge_all
from rxbp.ack.single import Single
from rxbp.ack.stopack import StopAck
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


class ConnectableObserver(Observer):
    def __init__(self, underlying: Observer, scheduler: Scheduler, subscribe_scheduler: Scheduler = None,
                 is_volatile: bool = None):
        self.underlying = underlying
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler  # todo: remove this
        self.is_volatile = is_volatile or False  # todo: remove this

        self.root_ack = AckSubject()
        self.connected_ack = self.root_ack
        self.is_connected = False
        self.was_canceled = False

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
            return StopAck()

    def on_error(self, err):
        self.was_canceled = True

        class ResultSingle(Single):
            def on_error(self, exc: Exception):
                raise NotImplementedError

            def on_next(_, v):
                if isinstance(v, ContinueAck):
                    self.underlying.on_error(err)

        # _observe_on(self.connected_ack, scheduler=self.scheduler).subscribe(ResultSingle())
        self.connected_ack.subscribe(ResultSingle())

    def on_completed(self):
        self.was_canceled = True

        class ResultSingle(Single):
            def on_error(self, exc: Exception):
                raise NotImplementedError

            def on_next(_, v):
                if isinstance(v, ContinueAck):
                    self.underlying.on_completed()

        # _observe_on(self.connected_ack, scheduler=self.scheduler).subscribe(ResultSingle())
        self.connected_ack.subscribe(ResultSingle())

    def dispose(self):
        self.was_canceled = True
        self.is_connected = True
