import traceback
from abc import ABC, abstractmethod
from typing import Callable, Any

from rx.disposable import Disposable

from rxbp.ack.continueack import continue_ack
from rxbp.ack.stopack import stop_ack
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class FlowableBase(ABC):
    """ See `Flowable` for more information.

    Two class are used to implement `Flowable`. `FlowableBase` implements the basic interface including the `subscribe`
    method. `Flowable` implements the `pipe` operator.
    """

    def subscribe(
            self,
            on_next: Callable[[Any], None] = None,
            on_error: Callable[[Any], None] = None,
            on_completed: Callable[[], None] = None,
            scheduler: Scheduler = None,
            subscribe_scheduler: Scheduler = None,
            observer: Observer = None,
    ) -> Disposable:
        """ Calling `subscribe` method starts some kind of process that

        start a chain reaction where downsream `Flowables`
        call the `subscribe` method of their linked upstream `Flowable` until
        the sources start emitting data. Once a `Flowable` is subscribed, we
        allow it to have mutable states where it make sense.
        """

        subscribe_scheduler_ = subscribe_scheduler or TrampolineScheduler()
        scheduler_ = scheduler or subscribe_scheduler_

        subscriber = Subscriber(scheduler=scheduler_, subscribe_scheduler=subscribe_scheduler_)

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

            observer = SubscribeObserver()

        observer_info = ObserverInfo(observer=observer)

        disposable = self.subscribe_(subscriber=subscriber, observer_info=observer_info)
        return disposable

    def subscribe_(self, subscriber: Subscriber, observer_info: ObserverInfo):
        subscription = self.unsafe_subscribe(subscriber=subscriber)

        def action(_, __):
            disposable = subscription.observable.observe(observer_info=observer_info)
            return disposable

        disposable = subscriber.subscribe_scheduler.schedule(action)
        return disposable

    @abstractmethod
    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        ...
