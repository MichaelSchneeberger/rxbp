from typing import Generic

from rxbp.flowable import Flowable
from rxbp.typing import ValueType


class MultiCastFlowable(Flowable[ValueType], Generic[ValueType]):
    def share(self) -> 'MultiCastFlowable':
        return MultiCastFlowable(self._share())
