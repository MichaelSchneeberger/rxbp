import threading

from rx.disposable import Disposable

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.subjects.subjectbase import SubjectBase


class RefCountObservable(Observable):
    def __init__(self, source: Observable, subject: SubjectBase):
        super().__init__()

        self.source = source
        self.subject = subject
        self.count = 0
        self.first_disposable = None
        self.lock = threading.RLock()

    def observe(self, observer: Observer):
        disposable = self.subject.observe(observer)

        with self.lock:
            self.count += 1
            if self.count == 1:
                self.first_disposable = self.source.observe(self.subject)

        def dispose():
            disposable.dispose()

            with self.lock:
                self.count -= 1
                if self.count == 0:
                    self.first_disposable.dispose()

        return Disposable(dispose)