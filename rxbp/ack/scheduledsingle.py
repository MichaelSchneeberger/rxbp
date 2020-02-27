import threading
from typing import List

from rx.core import typing
from rx.disposable import SerialDisposable

from rxbp.ack.single import Single


class ScheduledSingle(Single):
    def __init__(self, scheduler: typing.Scheduler, single: Single) -> None:
        super().__init__()

        self.scheduler = scheduler
        self.single = single

        self.lock = threading.RLock()
        self.is_acquired = False
        self.has_faulted = False
        self.queue: List[typing.Action] = []
        self.disposable = SerialDisposable()

    def on_next(self, value):
        def action():
            self.single.on_next(value)
        self.queue.append(action)
        self.ensure_active()

    def on_error(self, error: Exception):
        def action():
            self.single.on_error(error)
        self.queue.append(action)
        self.ensure_active()

    def ensure_active(self) -> None:
        is_owner = False

        with self.lock:
            if not self.has_faulted and self.queue:
                is_owner = not self.is_acquired
                self.is_acquired = True

        if is_owner:
            self.disposable.disposable = self.scheduler.schedule(self.run)

    def run(self, _, __) -> None:
        parent = self

        with self.lock:
            if parent.queue:
                work = parent.queue.pop(0)
            else:
                parent.is_acquired = False
                return

        try:
            work()
        except Exception:
            with self.lock:
                parent.queue = []
                parent.has_faulted = True
            raise

        self.scheduler.schedule(self.run)

    def dispose(self) -> None:
        self.is_stopped = True
        self.disposable.dispose()
