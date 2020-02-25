from dataclasses import dataclass
from typing import Generic

import rx
from rx.subject import Subject

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.typing import MultiCastValue


@dataclass
class MultiCastSubject(MultiCastBase[MultiCastValue], Generic[MultiCastValue]):
    subject = Subject()

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:
        return self.subject

    def on_next(self, val):
        self.subject.on_next(val)

    def on_error(self, exc):
        self.subject.on_error(exc)

    def on_completed(self):
        self.subject.on_completed()
