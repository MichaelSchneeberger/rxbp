from rx.core import Observer, AnonymousObserver, ObservableBase, Disposable, ObserverBase
from rx.core.notification import OnNext, OnCompleted
from rx.subjects import Subject, AsyncSubject
from rx.testing.reactive_assert import AssertList
from rx.testing.subscription import Subscription

from rxbackpressure.backpressuretypes.stoprequest import StopRequest
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
                # print('number of items {}'.format(number_of_items))
                future = AsyncSubject()
                # if number_of_items > 0:
                self.requests.append((future, number_of_items, number_of_items))
                update_requests()
                # else:
                #     raise NameError('Error')
                return future

        backpressure = HotBackpressure()
        self.backpressure = backpressure
        self.is_stopped = False

        def update_requests():
            if backpressure.requests and not self.is_stopped:
                response, current_nr, requested_nr = backpressure.requests[0]

                if isinstance(requested_nr, StopRequest):
                    self.is_stopped = True
                    response.on_next(requested_nr)
                    response.on_completed()
                    backpressure.requests = []

                elif self.buffer:
                    first_notification = self.buffer.pop(0)
                    first_notification.accept(self.observer)


                    if isinstance(first_notification, OnCompleted):
                        self.is_stopped = True
                        response.on_next(requested_nr - current_nr)
                        response.on_completed()
                    elif current_nr <= 1:
                        # finish current request
                        response.on_next(requested_nr)
                        response.on_completed()
                        backpressure.requests.pop(0)
                        update_requests()
                    else:
                        backpressure.requests[0] = (response, current_nr - 1, requested_nr)

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

    def subscribe(self, on_next=None, on_error=None, on_completed=None, observer=None, subscribe_bp=None,
                  scheduler=None):
        # Be forgiving and accept an un-named observer as first parameter
        if isinstance(on_next, BackpressureObserver):
            observer = on_next
        elif not observer or not isinstance(observer, BackpressureObserver):
            observer = AnonymousBackpressureObserver(on_next=on_next,
                                                     on_error=on_error,
                                                     on_completed = on_completed,
                                                     observer=observer,
                                                     subscribe_bp=subscribe_bp)

        return self._subscribe_core(observer, scheduler)

    def _subscribe_core(self, observer, scheduler):

        self.observer = observer
        observer.subscribe_backpressure(self.backpressure)

        observable = self
        self.subscriptions.append(Subscription(self.scheduler.clock))
        index = len(self.subscriptions) - 1

        def dispose_action():
            self.observer = None
            start = observable.subscriptions[index].subscribe
            end = observable.scheduler.clock
            observable.subscriptions[index] = Subscription(start, end)

        return Disposable.create(dispose_action)
