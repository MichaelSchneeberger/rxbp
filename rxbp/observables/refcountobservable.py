import threading
from typing import List, Optional

import rx
from rx.disposable import Disposable

from rxbp.init.initobserverinfo import init_observer_info
from rxbp.observable import Observable
from rxbp.observablesubjects.observablesubjectbase import ObservableSubjectBase
from rxbp.observerinfo import ObserverInfo


class RefCountObservable(Observable):
    def __init__(self, source: Observable, subject: ObservableSubjectBase):
        super().__init__()

        self.source = source
        self.subject = subject
        self.count = 0
        self.volatile_disposables: List[rx.typing.Disposable] = []
        self.first_disposable: Optional[rx.typing.Disposable] = None
        self.lock = threading.RLock()

    def observe(self, observer_info: ObserverInfo):
        disposable = self.subject.observe(observer_info)

        if observer_info.is_volatile:
            self.volatile_disposables.append(disposable)
            return disposable

        with self.lock:
            self.count += 1
            current_cound = self.count

        if current_cound == 1:
            subject_subscription = init_observer_info(self.subject, is_volatile=observer_info.is_volatile)
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

                for d in self.volatile_disposables:
                    d.dispose()

        return Disposable(dispose)