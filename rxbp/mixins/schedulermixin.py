from abc import ABC, abstractmethod

from rx.scheduler.scheduler import Scheduler as RxScheduler

from rxbp.mixins.executionmodelmixin import ExecutionModelMixin


class SchedulerMixin(RxScheduler, ABC):
    @abstractmethod
    def report_failure(self, exc: Exception):
        ...

    @abstractmethod
    def get_execution_model(self) -> ExecutionModelMixin:
        ...

    @property
    @abstractmethod
    def is_order_guaranteed(self) -> bool:
        ...

    @abstractmethod
    def sleep(self, seconds: float) -> None:
        ...