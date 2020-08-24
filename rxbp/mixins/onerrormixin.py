import types
from abc import ABC, abstractmethod

from rxbp.mixins.observermixin import ObserverMixin
from rxbp.mixins.postinitmixin import PostInitMixin


class OnErrorMixin(ObserverMixin, PostInitMixin, ABC):
    @property
    @abstractmethod
    def source(self) -> ObserverMixin:
        ...

    def __post_init__(self):
        super().__post_init__()

        self.on_error = types.MethodType(self.source.on_error.__func__, self)

    def on_error(self, exc: Exception):
        pass
