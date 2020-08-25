from dataclasses import dataclass
from typing import Generic

import rx
from rx.subject import Subject

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.typing import MultiCastItem


@dataclass
class MultiCastSubject(MultiCastMixin[MultiCastItem], Generic[MultiCastItem]):
    subject = Subject()

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> rx.typing.Observable[MultiCastItem]:
        return self.subject

    def on_next(self, val):
        self.subject.on_next(val)

    def on_error(self, exc):
        self.subject.on_error(exc)

    def on_completed(self):
        self.subject.on_completed()
