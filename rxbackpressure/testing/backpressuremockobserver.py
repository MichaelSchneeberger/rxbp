from rx.core import Observer, Disposable
from rx.core.notification import OnNext, OnError, OnCompleted

from rx.testing.recorded import Recorded
from rx.testing.reactive_assert import AssertList

from rxbackpressure.core.backpressureobserver import BackpressureObserver


class BackpressureMockObserver:

    def __init__(self, scheduler, backpressure_messages):
        self.scheduler = scheduler
        self.backpressure_messages = backpressure_messages
        self.messages = AssertList()
        self.backpressure = None

        def get_action(value):
            def action(scheduler, state):
                if self.backpressure:
                    self.backpressure.request(value)
                return Disposable.empty()
            return action

        for message in self.backpressure_messages:
            action = get_action(message.value)
            scheduler.schedule_absolute(message.time, action)

    def subscribe_backpressure(self, backpressure):
        # print('backpressure received')
        self.backpressure = backpressure

    def on_next(self, value):
        self.messages.append(Recorded(self.scheduler.clock, OnNext(value)))

    def on_error(self, exception):
        self.messages.append(Recorded(self.scheduler.clock, OnError(exception)))

    def on_completed(self):
        self.messages.append(Recorded(self.scheduler.clock, OnCompleted()))