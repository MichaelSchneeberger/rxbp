from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.share.sharedmemory import ShareSharedMemory


@dataclass
class ShareCancellation[U, E](Cancellable):
    id: int
    shared: ShareSharedMemory[U, E]

    @override
    def cancel(self, certificate: ContCert):
        # todo: implement cancel transition
        pass
