import threading

import rx
from rx import operators as rxop

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.subjects.subject import Subject


class SharedMultiCast(MultiCastBase):
    def __init__(
            self,
            source: MultiCastBase,
            subject: Subject,
    ):
        self.source = source
        self.subject = subject

        self._shared_source = None
        self._lock = threading.RLock()

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        with self._lock:
            if self._shared_source is None:
                self._shared_source = self.source.get_source(info=info).pipe(
                    rxop.multicast(subject=self.subject),
                    rxop.ref_count(),
                )

        return self._shared_source
