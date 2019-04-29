from abc import ABC, abstractmethod
from typing import Callable, Any, Set, Dict, Tuple, Optional, Generic

from rxbp.ack import continue_ack, Ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.selectors.bases import Base
from rxbp.subscriber import Subscriber
from rxbp.subscribers.anonymoussubscriber import AnonymousSubscriber
from rxbp.typing import ValueType


class FlowableBase(Generic[ValueType], ABC):
    FlowableReturnType = Tuple[Observable, Dict[Base, Optional[Observable]]]

    def __init__(self, base: Base = None, selectable_bases: Set[Base] = None):
        """
        :param base: two flowables with the the same base emit the same number of elements
        :param transformables: a set of bases different to the current base, a transformation is the capability to
        transform another Subscriptable to the current base, the actual transformations are defined in the `Observable`
        """

        self.base = base
        self.selectable_bases = selectable_bases or set()

    def subscribe_(self, subscriber: Subscriber, observer: Observer):
        def action(_, __):
            source_observable, _ = self.unsafe_subscribe(subscriber=subscriber)
            disposable = source_observable.observe(observer=observer)
            return disposable

        return subscriber.subscribe_scheduler.schedule(action)

    def subscribe(self,
                  on_next: Callable[[Any], None] = None,
                  on_error: Callable[[Any], None] = None,
                  on_completed: Callable[[], None] = None,
                  scheduler: Scheduler = None,
                  subscribe_scheduler: Scheduler = None):

        subscribe_scheduler_ = subscribe_scheduler or TrampolineScheduler()
        scheduler_ = scheduler or subscribe_scheduler_

        subscriber = AnonymousSubscriber(scheduler=scheduler_, subscribe_scheduler=subscribe_scheduler_)

        def on_next_with_ack(v):
            for value in v():
                on_next(value)
            return continue_ack

        on_next_ = (lambda v: continue_ack) if on_next is None else on_next_with_ack
        on_error_ = on_error or (lambda err: None)
        on_completed_ = on_completed or (lambda: None)

        observer = AnonymousObserver(on_next_func=on_next_, on_error_func=on_error_, on_completed_func=on_completed_)

        disposable = self.subscribe_(subscriber=subscriber, observer=observer)
        return disposable

    @abstractmethod
    def unsafe_subscribe(self, subscriber: Subscriber) -> 'FlowableBase.FlowableReturnType':
        ...
