from abc import abstractmethod, ABC

import rx

from rxbp.multicast.mixins.multicastobservablemixin import MultiCastObservableMixin
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo


class MultiCastObservable(MultiCastObservableMixin, ABC):
    pass
