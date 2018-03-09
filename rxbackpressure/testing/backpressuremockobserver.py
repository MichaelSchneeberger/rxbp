from rx.core import Observer, Disposable
from rx.core.notification import OnNext, OnError, OnCompleted

from rx.testing.recorded import Recorded
from rx.testing.reactive_assert import AssertList

from rxbackpressure.core.backpressureobserver import BackpressureObserver
from rxbackpressure.testing.notification import BPResponse


class BackpressureMockObserver(BackpressureObserver):

    def __init__(self, scheduler, backpressure_messages):
        super().__init__()

        self.scheduler = scheduler
        self.backpressure_messages = backpressure_messages
        self.messages = AssertList()
        self.bp_messages = AssertList()
        self.backpressure = None

        def get_action(value):
            def action(scheduler, state):
                if self.backpressure:
                    future = self.backpressure.request(value)
                    # print(future)
                    def request_done(value):
                        self.bp_messages.append(Recorded(self.scheduler.clock, BPResponse(value)))
                    # print('subscribe')
                    future.subscribe(request_done)
                return Disposable.empty()
            return action

        for message in self.backpressure_messages:
            action = get_action(message.value)
            scheduler.schedule_absolute(message.time, action)

    def subscribe_backpressure(self, backpressure):
        self.backpressure = backpressure
        return Disposable.empty()

    def on_next(self, value):
        self.messages.append(Recorded(self.scheduler.clock, OnNext(value)))

    def _on_next_core(self, value):
        pass

    def on_error(self, exception):
        self.messages.append(Recorded(self.scheduler.clock, OnError(exception)))

    def _on_error_core(self, error):
        pass

    def on_completed(self):
        self.messages.append(Recorded(self.scheduler.clock, OnCompleted()))

    def _on_completed_core(self):
        pass