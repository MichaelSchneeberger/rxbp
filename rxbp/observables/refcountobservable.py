import threading

from rx.disposable import Disposable

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observesubscription import ObserveSubscription
from rxbp.observablesubjects.observablesubjectbase import ObservableSubjectBase


class RefCountObservable(Observable):
    def __init__(self, source: Observable, subject: ObservableSubjectBase):
        super().__init__()

        self.source = source
        self.subject = subject
        self.count = 0
        self.volatile_disposables = []
        self.first_disposable = None
        self.lock = threading.RLock()

    def observe(self, subscription: ObserveSubscription):
        disposable = self.subject.observe(subscription)

        if subscription.is_volatile:
            self.volatile_disposables.append(disposable)
            return disposable

        with self.lock:
            self.count += 1
            current_cound = self.count

        if current_cound == 1:
            subject_subscription = ObserveSubscription(self.subject, is_volatile=subscription.is_volatile)
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