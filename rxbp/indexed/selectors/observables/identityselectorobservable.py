from rx.core.typing import Disposable

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.indexed.selectors.selectnext import select_next
from rxbp.indexed.selectors.selectcompleted import select_completed
from rxbp.typing import ElementType


class IdentitySelectorObservable(Observable):
    def __init__(
            self,
            source: Observable,
    ):
        self._source = source

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        observer_info = observer_info.observer

        class IdentityObserver(Observer):
            def on_next(self, elem: ElementType):
                def gen_select_msg():
                    for _ in elem:
                        yield select_next
                        yield select_completed

                # val = list(gen_select_msg())
                # ack = observer_info.on_next(val)

                ack = observer_info.on_next(gen_select_msg())
                return ack

            def on_error(self, exc: Exception):
                observer_info.on_next(exc)

            def on_completed(self):
                observer_info.on_completed()

        self._source.observe(observer_info.copy(
            observer=IdentityObserver(),
        ))

