from rxbp.ack import Continue, Stop
from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler


class DebugObservable(Observable):
    def __init__(self, source: Observable, name: str = None, on_next=None, on_completed=None, on_ack=None,
                 on_subscribe=None, on_raw_ack=None, on_next_exception=None):
        self.source = source
        self.name = name

        self.on_next_func = on_next or (lambda v: print('{}.on_next {}'.format(name, v)))
        self.on_completed_func = on_completed or (lambda: print('{}.on_completed'.format(name)))
        self.on_subscribe_func = on_subscribe or (lambda v: print('{}.on_subscribe {}'.format(name, v)))
        self.on_sync_ack = on_ack or (lambda v: print('{}.on_sync_ack {}'.format(name, v)))
        self.on_async_ack = on_ack or (lambda v: print('{}.on_async_ack {}'.format(name, v)))
        self.on_raw_ack = on_raw_ack or (lambda v: print('{}.on_raw_ack {}'.format(name, v)))
        self.on_next_exception = on_next_exception or (lambda v: print('{}.on_next exception raised "{}"'.format(name, v)))

    def observe(self, observer: Observer):
        if self.name is None:
            return self.source.observe(observer)

        self.on_subscribe_func(observer)

        def on_next(v):
            materialized = list(v())

            self.on_next_func(materialized)

            def gen():
                yield from materialized

            try:
                ack = observer.on_next(gen)
            except Exception as e:
                # self.on_next_exception(e)
                raise

            if isinstance(ack, Continue) or isinstance(ack, Stop):
                self.on_sync_ack(ack)
            else:
                self.on_raw_ack(ack)
                ack.subscribe(self.on_async_ack)
            return ack

        def on_completed():
            self.on_completed_func()
            return observer.on_completed()

        map_observer = AnonymousObserver(on_next_func=on_next, on_error_func=observer.on_error,
                                         on_completed_func=on_completed)
        return self.source.observe(map_observer)
