import types
from abc import ABC, abstractmethod

from rxbp.mixins.observermixin import ObserverMixin
from rxbp.mixins.postinitmixin import PostInitMixin


class OnCompletedMixin(ObserverMixin, PostInitMixin, ABC):
    @property
    @abstractmethod
    def source(self) -> ObserverMixin:
        ...

    def __post_init__(self):
        super().__post_init__()

        self.on_completed = types.MethodType(self.source.on_completed.__func__, self)

    def on_completed(self):
        pass
