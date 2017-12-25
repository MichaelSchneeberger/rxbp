from rx.core import Observer, Disposable
from rx.core.notification import OnNext, OnError, OnCompleted

from rx.testing.recorded import Recorded
from rx.testing.reactive_assert import AssertList

from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.backpressureobserver import BackpressureObserver
from rxbackpressure.testing.notification import BPResponse


class BackpressureMockSubject(BackpressureObserver, BackpressureObservable):

    def __init__(self, scheduler, backpressure_messages, selector):
        super().__init__()

        self.scheduler = scheduler
        self.backpressure_messages = backpressure_messages
        self.messages = AssertList()
        self.bp_messages = AssertList()
        self.backpressure = None
        self.observer = None
        self.selector = selector

        def get_action(value):
            def action(scheduler, state):
                if self.backpressure:
                    future = self.backpressure.request(value)
                    future.subscribe(
                        lambda value: self.bp_messages.append(Recorded(self.scheduler.clock, BPResponse(value))))
                return Disposable.empty()
            return action

        for message in self.backpressure_messages:
            action = get_action(message.value)
            scheduler.schedule_absolute(message.time, action)

    def _subscribe_core(self, observer):
        self.observer = observer

    def subscribe_backpressure(self, backpressure):
        self.backpressure = backpressure

    def on_next(self, value):
        sel_value = self.selector(value)
        self.messages.append(Recorded(self.scheduler.clock, OnNext(sel_value)))
        if self.observer:
            self.observer.on_next(value)

    def on_error(self, exception):
        self.messages.append(Recorded(self.scheduler.clock, OnError(exception)))
        if self.observer:
            self.observer.on_error(exception)

    def on_completed(self):
        self.messages.append(Recorded(self.scheduler.clock, OnCompleted()))
        if self.observer:
            self.observer.on_completed()

    def _on_next_core(self, value):
        pass

    def _on_error_core(self, error):
        pass

    def _on_completed_core(self):
        pass