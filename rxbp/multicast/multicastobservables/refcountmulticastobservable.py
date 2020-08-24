import threading
from dataclasses import dataclass

from rx.disposable import Disposable

from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.subjects.multicastobservablesubject import MultiCastObservableSubject
from rxbp.observable import Observable


@dataclass
class RefCountMultiCastObservable(Observable):
    source: MultiCastObservable
    subject: MultiCastObservableSubject
    lock: threading.RLock
    count: int

    def observe(self, observer_info: MultiCastObserverInfo):
        disposable = self.subject.observe(observer_info)

        with self.lock:
            self.count += 1
            current_cound = self.count

        if current_cound == 1:
            subject_subscription = observer_info.copy(observer=self.subject)
            self.first_disposable = self.source.observe(subject_subscription)

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