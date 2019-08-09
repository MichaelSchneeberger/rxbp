from rxbp.ack.ackimpl import continue_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observesubscription import ObserveSubscription


class ToListObservable(Observable):
    def __init__(self, source: Observable):
        super().__init__()

        self.source = source

    def observe(self, subscription: ObserveSubscription):
        observer = subscription.observer
        queue = [[]]

        def on_completed():
            def gen():
                yield queue[0]

            _ = observer.on_next(gen)
            observer.on_completed()

        def on_next(v):
            queue[0] += list(v())

            return continue_ack

        class ToListObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                return on_completed()

        map_observer = ToListObserver()
        map_subscription = ObserveSubscription(map_observer, is_volatile=subscription.is_volatile)
        return self.source.observe(map_subscription)
