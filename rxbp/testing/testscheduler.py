from rx.scheduler.virtualtimescheduler import VirtualTimeScheduler

from rxbp.mixins.executionmodelmixin import ExecutionModelMixin
from rxbp.scheduler import SchedulerBase, UncaughtExceptionReport, BatchedExecution


class TestScheduler(VirtualTimeScheduler, SchedulerBase):
    def __init__(self):
        super().__init__()
        self.r = UncaughtExceptionReport()
        self.execution_model = BatchedExecution(16)

    def idle(self):
        return True

    @property
    def is_order_guaranteed(self) -> bool:
        return True

    def report_failure(self, exc: Exception):
        return self.r.report_failure(exc)

    def get_execution_model(self) -> ExecutionModelMixin:
        return self.execution_model

    def schedule_required(self):
        return True

    @staticmethod
    def add(absolute, relative):
        return absolute + relative

    def sleep(self, seconds: float):
        return self.advance_by(seconds)
