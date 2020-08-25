import traceback
from abc import ABC
from typing import Callable, Any

from rx.disposable import Disposable

from rxbp.acknowledgement.continueack import continue_ack
from rxbp.acknowledgement.stopack import stop_ack
from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler


class ObserveMixin(ABC):
    def _observe(
            self,
            observable: Observable = None,
            on_next: Callable[[Any], None] = None,
            on_error: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            subscribe_scheduler: Scheduler = None,
            observer: Observer = None,
    ) -> Disposable:
        def action(_, __):

            if not isinstance(observer, Observer):
                def default_on_error(exc: Exception):
                    traceback.print_exception(type(exc), exc, exc.__traceback__)

                on_next_ = (lambda v: None) if on_next is None else on_next
                on_error_ = default_on_error if on_error is None else on_error
                on_completed_ = on_completed or (lambda: None)

                class SubscribeObserver(Observer):
                    def on_next(self, v):
                        try:
                            for value in v:
                                on_next_(value)
                            return continue_ack
                        except Exception as exc:
                            on_error_(exc)
                            return stop_ack

                    def on_error(self, exc: Exception):
                        on_error_(exc)

                    def on_completed(self):
                        on_completed_()

                observer_ = SubscribeObserver()
            else:
                observer_ = observer

            observer_info = init_observer_info(observer=observer_)
            disposable = observable.observe(observer_info=observer_info)

            return disposable

        return subscribe_scheduler.schedule(action)
