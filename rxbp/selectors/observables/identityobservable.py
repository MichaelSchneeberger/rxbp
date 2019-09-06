from rx.core.typing import Disposable
from rxbp.ack.ackbase import AckBase
from rxbp.ack.ackimpl import Continue
from rxbp.ack.acksubject import AckSubject
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.selectionmsg import select_next, select_completed
from rxbp.typing import ElementType


class IdentityObservable(Observable):
    def __init__(
            self,
            source: Observable,
    ):
        self._source = source

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        observer = observer_info.observer

        class IdentityObserver(Observer):
            def on_next(self, _: ElementType):
                def on_acknowledgment(a: AckBase):
                    if isinstance(a, Continue):
                        inner_ack = observer.on_next(select_completed)
                        inner_ack.subscribe(ack_subject)

                ack_subject = AckSubject()
                ack = observer.on_next(select_next)
                ack.subscribe(on_acknowledgment)

                return ack_subject

            def on_error(self, exc: Exception):
                observer.on_next(exc)

            def on_completed(self):
                observer.on_completed()

        self._source.observe(observer_info.copy(observer=IdentityObserver()))

