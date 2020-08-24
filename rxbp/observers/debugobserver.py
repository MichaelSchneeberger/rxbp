from dataclasses import dataclass
from traceback import FrameSummary
from typing import Optional, Callable, Any, List

from rxbp.ack.continueack import ContinueAck
from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.single import Single
from rxbp.ack.stopack import StopAck, stop_ack
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class DebugObserver(Observer):
    source: Observer
    name: Optional[str]
    on_next_func: Callable[[Any], AckMixin]
    on_completed_func: Callable[[], None]
    on_error_func: Callable[[Exception], None]
    on_sync_ack: Callable[[AckMixin], None]
    on_async_ack: Callable[[AckMixin], None]
    on_subscribe: Callable[[ObserverInfo], None]
    on_raw_ack: Callable[[AckMixin], None]
    stack: List[FrameSummary]
    has_scheduled_next: bool

    def on_next(self, elem: ElementType):
        if not self.has_scheduled_next:
            raise Exception(to_operator_exception(
                message='Element received before subscribe scheduler advanced',
                stack=self.stack,
            ))

        try:
            materialized = list(elem)
        except Exception as exc:
            self.on_error(exc)
            self.source.on_error(exc)
            return stop_ack

        self.on_next_func(materialized)

        ack = self.source.on_next(materialized)

        if isinstance(ack, ContinueAck) or isinstance(ack, StopAck):
            self.on_sync_ack(ack)
        else:
            self.on_raw_ack(ack)

            class ResultSingle(Single):
                def on_next(_, elem):
                    self.on_async_ack(elem)

                def on_error(self, exc: Exception):
                    pass

            ack.subscribe(ResultSingle())
        return ack

    def on_error(self, exc):
        self.on_error_func(exc)
        self.source.on_error(exc)

    def on_completed(self):
        self.on_completed_func()
        return self.source.on_completed()