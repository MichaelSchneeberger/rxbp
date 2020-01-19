from abc import ABC, abstractmethod

from rx.disposable import Disposable

from rxbp.ack.single import Single


class AckMixin(ABC):
    """ An acknowledgment is what is return by a on_next method call to potentially
    back-pressure the upstream Observable.

    An acknowledgment is build similarly to a rx Observable by having a `subscribe`
    method defined.
    """

    # a synchronous acknowledgment (compared to a asynchronous) is one that is
    # immediate, e.g. without back-pressure.
    is_sync = False

    @abstractmethod
    def subscribe(self, single: Single) -> Disposable:
        ...
