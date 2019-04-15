import functools
import itertools
from typing import Callable, Any

from rx import config
from rx.core import Disposable

from rxbp.ack import Continue, Ack
from rxbp.internal.indexing import on_next_idx, on_completed_idx
from rxbp.internal.indexingop import merge_indexes
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observers.dummyobserver import DummyObserver
from rxbp.scheduler import Scheduler
from rxbp.subjects.publishsubject import PublishSubject


class FilterObservable(Observable):
    def __init__(self, source: Observable, predicate: Callable[[Any], bool], scheduler: Scheduler):

        super().__init__()

        self.selector = PublishSubject(scheduler=scheduler)

        self.source = source
        self.predicate = predicate

    def observe(self, observer: Observer):
        def on_next(v):
            def gen_filtered_iterable():
                for e in v():
                    if self.predicate(e):
                        yield True, e
                    else:
                        yield False, e

            filtered_values = list(gen_filtered_iterable())
            should_run = functools.reduce(lambda acc, v: acc or v[0], filtered_values, False)

            def gen_selector():
                for sel, elem in filtered_values:
                    if sel:
                        yield on_next_idx
                    yield on_completed_idx

            sel_ack = self.selector.on_next(gen_selector)

            if should_run:
                def gen_output():
                    for sel, elem in filtered_values:
                        if sel:
                            yield elem

                ack1: Ack = observer.on_next(gen_output)

                return ack1.merge_ack(sel_ack)
            else:
                return sel_ack

        source = self

        class FilterObserver(Observer):
            def on_next(self, v):
                return on_next(v)

            def on_error(self, exc):
                source.selector.on_completed()
                return observer.on_error(exc)

            def on_completed(self):
                source.selector.on_completed()
                return observer.on_completed()

        filter_observer = FilterObserver()
        return self.source.observe(filter_observer)
