import threading
from dataclasses import dataclass
from traceback import FrameSummary
from typing import List, Optional

import rx
from rx.disposable import Disposable

from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observable import Observable
from rxbp.observablesubjects.observablesubjectbase import ObservableSubjectBase
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class RefCountObservable(Observable):
    source: Observable
    subject: ObservableSubjectBase
    subscribe_scheduler: TrampolineScheduler
    stack: List[FrameSummary]

    def __post_init__(self):
        self.count = 0
        self.volatile_disposables: List[rx.typing.Disposable] = []
        self.first_disposable: Optional[rx.typing.Disposable] = None
        self.lock = threading.RLock()
        self.scheduled_next = False

    def observe(self, observer_info: ObserverInfo):
        disposable = self.subject.observe(observer_info)

        if observer_info.is_volatile:
            self.volatile_disposables.append(disposable)
            return disposable

        with self.lock:
            self.count += 1
            current_cound = self.count

        if current_cound == 1:
            if self.subscribe_scheduler.idle:
                raise Exception(to_operator_exception(
                    message='observe method call should be scheduled on subscribe scheduler',
                    stack=self.stack,
                ))

            def action(_, __):
                self.scheduled_next = True

            self.subscribe_scheduler.schedule(action)

            subject_subscription = init_observer_info(self.subject, is_volatile=observer_info.is_volatile)
            self.first_disposable = self.source.observe(subject_subscription)

        else:
            if self.scheduled_next:
                raise Exception(to_operator_exception(
                    message='subsequent subscribe call has been delayed, make sure to not delay Flowable subscriptions',
                    stack=self.stack,
                ))

        def dispose():
            disposable.dispose()

            with self.lock:
                self.count -= 1
                if self.count == 0:
                    dispose_all = True
                else:
                    dispose_all = False

            if dispose_all:
                self.first_disposable.dispose()

                for d in self.volatile_disposables:
                    d.dispose()

        return Disposable(dispose)