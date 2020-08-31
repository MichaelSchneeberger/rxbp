import functools
from dataclasses import dataclass
from typing import Callable, Any

from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.operators.mergeack import merge_ack
from rxbp.acknowledgement.stopack import stop_ack
from rxbp.observablesubjects.publishobservablesubject import PublishObservableSubject
from rxbp.observer import Observer
from rxbp.indexed.selectors.selectnext import select_next
from rxbp.indexed.selectors.selectcompleted import select_completed
from rxbp.typing import ElementType


@dataclass
class IndexedFilterObserver(Observer):
    observer: Observer
    predicate: Callable[[Any], bool]
    selector_subject: PublishObservableSubject

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

        sel_ack = self.selector_subject.on_next(gen_selector())

        if should_run:
            def gen_output():
                for sel, elem in filtered_values:
                    if sel:
                        yield elem

            ack1: Ack = self.observer.on_next(gen_output())

            return merge_ack(ack1, sel_ack)
        else:
            return sel_ack

    def on_error(self, exc):
        self.selector_subject.on_completed()
        return self.observer.on_error(exc)

    def on_completed(self):
        self.selector_subject.on_completed()
        return self.observer.on_completed()
