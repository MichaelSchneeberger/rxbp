from rx.concurrency import VirtualTimeScheduler

from rxbp.scheduler import SchedulerBase, ExecutionModel, UncaughtExceptionReport, BatchedExecution


class TestScheduler(SchedulerBase, VirtualTimeScheduler):
    def __init__(self):
        super().__init__()
        self.r = UncaughtExceptionReport()
        self.execution_model = BatchedExecution(16)

    def report_failure(self, exc: Exception):
        return self.r.report_failure(exc)

    def get_execution_model(self) -> ExecutionModel:
        return self.execution_model

    @staticmethod
    def add(absolute, relative):
        return absolute + relative
