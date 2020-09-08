import threading
from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.subjects.multicastobservablesubject import MultiCastObservableSubject
from rxbp.observable import Observable
from rxbp.scheduler import Scheduler
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class RefCountMultiCastObservable(Observable):
    source: MultiCastObservable
    subject: MultiCastObservableSubject
    multicast_scheduler: TrampolineScheduler
    stack: List[FrameSummary]

    def __post_init__(self):
        self.lock = threading.RLock()
        self.count = 0
        self.scheduled_next = False

    def observe(self, observer_info: MultiCastObserverInfo):
        disposable = self.subject.observe(observer_info)

        with self.lock:
            self.count += 1
            current_cound = self.count

        if current_cound == 1:
            # if self.multicast_scheduler.idle:
            #     raise Exception(to_operator_exception(
            #         message='observe method call should be scheduled on multicast scheduler',
            #         stack=self.stack,
            #     ))

            def action(_, __):
                self.scheduled_next = True

            self.multicast_scheduler.schedule(action)

            subject_subscription = observer_info.copy(observer=self.subject)
            self.first_disposable = self.source.observe(subject_subscription)

        # else:
        #     if self.scheduled_next:
        #         raise Exception(to_operator_exception(
        #             message='subsequent subscribe call has been delayed, make sure to not delay MultiCasts subscriptions',
        #             stack=self.stack,
        #         ))

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

                # for d in self.volatile_disposables:
                #     d.dispose()

        return Disposable(dispose)