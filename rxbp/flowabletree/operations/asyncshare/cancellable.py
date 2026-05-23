from dataclasses import dataclass
from typing import override

from contcert.typing import ContCert

from rxbp.cancellable import Cancellable
from rxbp.flowabletree.operations.asyncshare.sharedmemory import AsyncShareSharedMemory


@dataclass
class AsyncShareCancellable[U, E](Cancellable):
    id: int
    shared: AsyncShareSharedMemory[U, E]

    @override
    def cancel(self, certificate: ContCert):
        # todo: implement cancel transition
        pass
