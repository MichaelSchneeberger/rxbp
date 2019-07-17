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
        self.volatile_disposables = []
        self.first_disposable = None
        self.lock = threading.RLock()

    def observe(self, observer: Observer):
        disposable = self.subject.observe(observer)

        if observer.is_volatile:
            self.volatile_disposables.append(disposable)
            return disposable

        with self.lock:
            self.count += 1
            current_cound = self.count

        if current_cound == 1:
            self.first_disposable = self.source.observe(self.subject)

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