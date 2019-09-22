import functools
from typing import Callable, Any

from rxbp.ack.ack import Ack
from rxbp.ack.ackimpl import stop_ack
from rxbp.ack.merge import _merge
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.selectionmsg import select_next, select_completed
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.typing import ElementType


class FilterObservable(Observable):
    def __init__(self, source: Observable, predicate: Callable[[Any], bool], scheduler: Scheduler):

        super().__init__()

        self.selector = PublishOSubject(scheduler=scheduler)

        self.source = source
        self.predicate = predicate

    def observe(self, observer_info: ObserverInfo):
        observer = observer_info.observer

        def on_next(elem: ElementType):
            def gen_filtered_iterable():
                for e in elem:
                    if self.predicate(e):
                        yield True, e
                    else:
                        yield False, e

            try:
                # buffer elemenets
                filtered_values = list(gen_filtered_iterable())
            except Exception as exc:
                observer.on_error(exc)
                return stop_ack

            should_run = functools.reduce(lambda acc, v: acc or v[0], filtered_values, False)

            def gen_selector():
                for sel, elem in filtered_values:
                    if sel:
                        yield select_next
                    yield select_completed

            sel_ack = self.selector.on_next(gen_selector())

            if should_run:
                def gen_output():
                    for sel, elem in filtered_values:
                        if sel:
                            yield elem

                ack1: Ack = observer.on_next(gen_output())

                return _merge(ack1, sel_ack)
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

        filter_subscription = observer_info.copy(FilterObserver())
        return self.source.observe(filter_subscription)
