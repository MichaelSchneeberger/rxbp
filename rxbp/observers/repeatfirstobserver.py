from dataclasses import dataclass

from rxbp.acknowledgement.continueack import ContinueAck, continue_ack
from rxbp.acknowledgement.single import Single
from rxbp.acknowledgement.stopack import StopAck, stop_ack
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


@dataclass
class RepeatFirstObserver(Observer):
    next_observer: Observer
    scheduler: Scheduler

    def on_next(self, elem: ElementType):
        if isinstance(elem, list):
            first_elem = elem[0]
        else:
            try:
                first_elem = next(elem)
            except StopIteration:
                # empty element, wait for next
                return continue_ack
            except Exception as exc:
                self.next_observer.on_error(exc)
                return stop_ack

        def gen_batch():
            while True:
                yield first_elem

        batch = gen_batch()

        def action(_, __):
            while True:
                ack = self.next_observer.on_next(batch)

                if isinstance(ack, ContinueAck):
                    pass
                elif isinstance(ack, StopAck):
                    break
                else:
                    class RepeatFirstSingle(Single):
                        def on_next(_, elem):
                            if isinstance(elem, ContinueAck):
                                self.scheduler.schedule(action)

                    ack.subscribe(RepeatFirstSingle())
                    break

        self.scheduler.schedule(action)
        return stop_ack

    def on_error(self, exc):
        return self.next_observer.on_error(exc)

    def on_completed(self):
        pass
