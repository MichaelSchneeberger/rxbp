from typing import Callable, Any, List

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler


class ConcatObservable(Observable):
    def __init__(self, sources: List[Observable]):
        super().__init__()

        self.sources = sources

    def observe(self, observer: Observer):
        class ConcatObserver(Observer):
            def __init__(self, sources: List[Observable]):
                self.sources = sources
                self.ack = None

            def on_next(self, v):
                self.ack = observer.on_next(v)
                return self.ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                if self.sources:
                    def observe_next():
                        new_observer = ConcatObserver(self.sources[1:])
                        first_source = self.sources[0]
                        first_source.observe(new_observer)       # todo: handle disposable

                    if self.ack:
                        self.ack.subscribe(lambda _: observe_next())        # todo: handle stop ack
                    else:
                        observe_next()
                else:
                    observer.on_completed()

        concat_observer = ConcatObserver(self.sources[1:])

        first_source = self.sources[0]
        return first_source.observe(concat_observer)
