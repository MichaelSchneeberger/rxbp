import rx
from rx import operators as rxop

from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.subjects.subject import Subject


class SharedMultiCast(MultiCastBase):
    def __init__(
            self,
            source: MultiCastBase,
            subject: Subject()
    ):
        self.source = source
        self.subject = subject

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        shared_source = self.source.get_source(info=info).pipe(
            rxop.multicast(subject=self.subject),
            rxop.ref_count(),
        )

        return shared_source
