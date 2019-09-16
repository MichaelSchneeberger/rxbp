from rxbp.ack.ackimpl import continue_ack
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo


class ToListObservable(Observable):
    def __init__(self, source: Observable):
        super().__init__()

        self.source = source

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer
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

        to_list_observer = ToListObserver()
        return self.source.observe(observer_info.copy(observer=to_list_observer))
