from abc import ABC, abstractmethod

from rx.scheduler.scheduler import Scheduler as RxScheduler


class ExecutionModel:
    def next_frame_index(self, current: int) -> int:
        raise NotImplementedError


class BatchedExecution(ExecutionModel):
    def __init__(self, batch_size: int):
        self.recommended_batch_size = batch_size
        self.batched_execution_modulus = self.recommended_batch_size - 1

    def next_frame_index(self, current: int) -> int:
        return (current + 1) & self.batched_execution_modulus


class UncaughtExceptionReport:
    def report_failure(self, exc: Exception):
        raise exc


class Scheduler(RxScheduler, ABC):
    @abstractmethod
    def report_failure(self, exc: Exception):
        ...

    @abstractmethod
    def get_execution_model(self) -> ExecutionModel:
        ...

    @property
    @abstractmethod
    def is_order_guaranteed(self) -> bool:
        ...

    @abstractmethod
    def sleep(self, seconds: float) -> None:
        ...


class SchedulerBase(Scheduler, ABC):
    def __init__(self, r: UncaughtExceptionReport = None, execution_model: ExecutionModel = None):
        super().__init__()

        self.r = r or UncaughtExceptionReport()
        self.execution_model = execution_model or BatchedExecution(256)

    def report_failure(self, exc: Exception):
        return self.r.report_failure(exc)

    def get_execution_model(self) -> ExecutionModel:
        return self.execution_model


