import threading
from typing import Any, Optional, List

from rx.disposable import Disposable
from rx.internal import DisposedException

from rxbp.acknowledgement.mixins.ackmergemixin import AckMergeMixin
from rxbp.acknowledgement.ack import Ack
from rxbp.acknowledgement.operators.mergeack import merge_ack
from rxbp.acknowledgement.single import Single


class AckSubject(AckMergeMixin, Ack, Single):
    def __init__(self) -> None:
        super().__init__()

        self._lock = threading.RLock()

        self.is_disposed = False
        self.singles: List[Single] = []
        self.exception: Optional[Exception] = None

        self._value = (False, None)

    @property
    def has_value(self):
        return self._value[0]

    @property
    def value(self):
        return self._value[1]

    def check_disposed(self) -> None:
        if self.is_disposed:
            raise DisposedException()

    def subscribe(self, single: Single) -> Disposable:
        assert isinstance(single, Single), f'"{single}" is not of type Single'

        with self._lock:
            self.check_disposed()
            self.singles.append(single)

            # ex = self.exception
            has_value, value = self._value

        # if ex:
        #     single.on_error(ex)
        if has_value:
            single.on_next(value)

        return Disposable()

    def on_next(self, value: Any) -> None:

        with self._lock:
            singles = self.singles.copy()
            self.singles.clear()
            self._value = (True, value)

        for single in singles:
            single.on_next(value)

    # def on_error(self, error: Exception) -> None:
    #
    #     with self._lock:
    #         self.check_disposed()
    #         singles = self.singles.copy()
    #         self.singles.clear()
    #         self.exception = error
    #
    #     for single in singles:
    #         single.on_error(error)

    def merge(self, other: Ack):
        return merge_ack(self, other)

    def dispose(self) -> None:

        with self._lock:
            self.is_disposed = True
            self.singles = []
            self.exception = None

            self._value = (False, None)
