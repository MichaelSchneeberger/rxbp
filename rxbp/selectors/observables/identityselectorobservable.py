from rx.core.typing import Disposable
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.selectionmsg import select_next, select_completed
from rxbp.typing import ElementType


class IdentitySelectorObservable(Observable):
    def __init__(
            self,
            source: Observable,
    ):
        self._source = source

    def observe(self, observer_info: ObserverInfo) -> Disposable:
        observer = observer_info.observer

        class IdentityObserver(Observer):
            def on_next(self, elem: ElementType):
                # class IdentitySingle(Single):
                #     def on_error(self, exc: Exception):
                #         pass
                #
                #     def on_next(self, a):
                #         if isinstance(a, Continue):
                #             inner_ack = observer.on_next(select_completed)
                #             inner_ack.subscribe(ack_subject)

                buffer = list(elem())
                # print('IdentityObserver.on_next({})'.format(buffer))
                # print('IdentityObserver.observer = {}'.format(observer))
                select_msg_buffer = [select_next for _ in range(len(buffer))] + [select_completed]

                def gen():
                    yield from select_msg_buffer

                ack = observer.on_next(gen)
                return ack

            def on_error(self, exc: Exception):
                observer.on_next(exc)

            def on_completed(self):
                observer.on_completed()

        self._source.observe(observer_info.copy(observer=IdentityObserver()))

