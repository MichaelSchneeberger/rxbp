import functools
from typing import Callable, Any

from rxbp.ack.mixins.ackmixin import AckMixin
from rxbp.ack.operators.merge import _merge
from rxbp.ack.stopack import stop_ack
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.observer import Observer
from rxbp.selectors.selectionmsg import select_next, select_completed
from rxbp.typing import ElementType


class FilterObserver(Observer):
    def __init__(
            self,
            observer: Observer,
            predicate: Callable[[Any], bool],
            selector: PublishOSubject,
    ):
        self.observer = observer
        self.predicate = predicate
        self.selector = selector

    def on_next(self, elem: ElementType):
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
            self.observer.on_error(exc)
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

            ack1: AckMixin = self.observer.on_next(gen_output())

            return _merge(ack1, sel_ack)
        else:
            return sel_ack

    def on_error(self, exc):
        self.selector.on_completed()
        return self.observer.on_error(exc)

    def on_completed(self):
        self.selector.on_completed()
        return self.observer.on_completed()
