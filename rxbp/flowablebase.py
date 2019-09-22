import traceback
from abc import ABC, abstractmethod
from typing import Callable, Any, Tuple, Generic

from rx.disposable import Disposable
from rxbp.ack.ackimpl import continue_ack, stop_ack
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscription import Subscription
from rxbp.subscriber import Subscriber
from rxbp.typing import ValueType


class FlowableBase(Generic[ValueType], ABC):

    def __init__(self):
        """
        :param base: two flowables with the the same base emit the same number of elements
        :param transformables: a set of bases different to the current base, a transformation is the capability to
        transform another Subscriptable to the current base, the actual transformations are defined in the `Observable`
        """

        pass

    def subscribe_(self, subscriber: Subscriber, observer_info: ObserverInfo):
        def action(_, __):
            subscription = self.unsafe_subscribe(subscriber=subscriber)
            disposable = subscription.observable.observe(observer_info=observer_info)
            return disposable

        disposable = subscriber.subscribe_scheduler.schedule(action)
        return disposable

    def subscribe(
            self,
            on_next: Callable[[Any], None] = None,
            on_error: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            scheduler: Scheduler = None,
            subscribe_scheduler: Scheduler = None
    ) -> Disposable:

        subscribe_scheduler_ = subscribe_scheduler or TrampolineScheduler()
        scheduler_ = scheduler or subscribe_scheduler_

        subscriber = Subscriber(scheduler=scheduler_, subscribe_scheduler=subscribe_scheduler_)

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

        subscription = ObserverInfo(observer=SubscribeObserver())

        disposable = self.subscribe_(subscriber=subscriber, observer_info=subscription)
        return disposable

    @abstractmethod
    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        ...
