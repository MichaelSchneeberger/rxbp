from rx.core import Observer, AnonymousObserver, ObservableBase, Disposable, ObserverBase
from rx.testing.reactive_assert import AssertList
from rx.testing.subscription import Subscription

from rxbackpressure import BlockingFuture
from rxbackpressure.core.anonymousbackpressureobserver import AnonymousBackpressureObserver
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.backpressureobserver import BackpressureObserver


class BPHotObservable(BackpressureObservable):
    def __init__(self, scheduler, messages):
        super().__init__()

        self.scheduler = scheduler
        self.messages = messages
        self.subscriptions = AssertList()
        self.observer = None
        self.buffer = []

        class HotBackpressure(BackpressureBase):
            def __init__(self):
                self.requests = []

            def request(self, number_of_items):
                future = BlockingFuture()
                if number_of_items > 0:
                    self.requests.append((future, number_of_items, number_of_items))
                    update_requests()
                else:
                    raise NameError('Error')
                return future

        backpressure = HotBackpressure()
        self.backpressure = backpressure

        def update_requests():
            if backpressure.requests and self.buffer:
                first_request = backpressure.requests[0]
                first_notification = self.buffer.pop(0)
                first_notification.accept(self.observer)
                if first_request[1] is 1:
                    future = first_request[0]
                    future.set(first_request[2])
                    backpressure.requests.pop(0)
                else:
                    backpressure.requests[0] = (first_request[0], first_request[1] - 1, first_request[2])

        def get_action(notification):
            def action(scheduler, state):
                if self.observer:
                    self.buffer.append(notification)
                    update_requests()

                return Disposable.empty()
            return action

        for message in self.messages:
            notification = message.value

            # Warning: Don't make closures within a loop
            action = get_action(notification)
            scheduler.schedule_absolute(message.time, action)

    def subscribe(self, on_next=None, on_error=None, on_completed=None, observer=None, subscribe_bp=None):
        # Be forgiving and accept an un-named observer as first parameter
        if isinstance(on_next, BackpressureObserver):
            observer = on_next
        elif not observer:
            observer = AnonymousBackpressureObserver(on_next=on_next,
                                                     on_error=on_error,
                                                     on_completed = on_completed,
                                                     observer=observer,
                                                     subscribe_bp=subscribe_bp)

        return self._subscribe_core(observer)

    def _subscribe_core(self, observer):

        self.observer = observer
        observer.subscribe_backpressure(self.backpressure)

        observable = self
        self.subscriptions.append(Subscription(self.scheduler.clock))
        index = len(self.subscriptions) - 1

        def dispose_action():
            self.observer = None
            start = observable.subscriptions[index].subscribe
            end = observable.scheduler.clock
            print(end)
            observable.subscriptions[index] = Subscription(start, end)

        return Disposable.create(dispose_action)
