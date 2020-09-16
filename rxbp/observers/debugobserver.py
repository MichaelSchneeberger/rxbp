from dataclasses import dataclass
from traceback import FrameSummary
from typing import Optional, Callable, Any, List

from rxbp.acknowledgement.continueack import ContinueAck, continue_ack
from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.single import Single
from rxbp.acknowledgement.stopack import StopAck, stop_ack
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.typing import ElementType
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class DebugObserver(Observer):
    source: Observer
    name: Optional[str]
    on_next_func: Callable[[Any], Ack]
    on_completed_func: Callable[[], None]
    on_error_func: Callable[[Exception], None]
    on_sync_ack: Callable[[Ack], None]
    on_async_ack: Callable[[Ack], None]
    on_raw_ack: Callable[[Ack], None]
    stack: List[FrameSummary]

    # def __post_init__(self):
    #     self.has_scheduled_next = False

    def on_next(self, elem: ElementType):
        # if not self.has_scheduled_next:
        #     raise Exception(to_operator_exception(
        #         message='Element received before subscribe scheduler advanced',
        #         stack=self.stack,
        #     ))

        try:
            materialized = list(elem)

            if len(materialized) == 0:
                return continue_ack

            for elem in materialized:
                self.on_next_func(elem)

            ack = self.source.on_next(materialized)

        except Exception as exc:
            self.on_error(exc)
            self.source.on_error(exc)
            return stop_ack

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