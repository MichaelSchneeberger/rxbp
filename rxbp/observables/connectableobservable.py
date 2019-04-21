from rx.core import Disposable
from rx.disposables import CompositeDisposable

from rxbp.observable import Observable
from rxbp.scheduler import Scheduler
from rxbp.subjects.publishsubject import PublishSubject


class ConnectableObservable:

    def __init__(self, source, scheduler: Scheduler, subscribe_scheduler: Scheduler, subject=None):
        super().__init__()
        self.source = source
        self.subject = subject or PublishSubject(scheduler=scheduler)
        self.has_subscription = False
        self.subscription = None
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler

    def connect(self):
        """Connects the observable."""

        if not self.has_subscription:
            self.has_subscription = True

            def dispose():
                self.has_subscription = False

            disposable = self.source.observe(self.subject)
            self.subscription = CompositeDisposable(disposable, Disposable.create(dispose))

        return self.subscription

    def ref_count(self):

        source = self

        class RefCountObservable(Observable):
            def __init__(self, source):
                self.source = source
                self.count = 0
                self.first_disposable = None

            def observe(self, observer):
                self.count += 1
                disposable = self.source.subject.observe(observer)
                if self.count == 1:
                    self.first_disposable = self.source.connect()

                def dispose():
                    disposable.dispose()
                    self.count -= 1
                    if self.count == 0:
                        self.first_disposable.dispose()

                return Disposable.create(dispose)

        return RefCountObservable(self)
