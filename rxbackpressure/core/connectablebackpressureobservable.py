from rx.core import Disposable
from rx.disposables import CompositeDisposable

from rxbackpressure.core.backpressureobservable import BackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


class ConnectableObservableBase:

    def __init__(self, source, subject):
        super().__init__()
        self.source = source
        self.subject = subject
        self.has_subscription = False
        self.subscription = None

    def _subscribe_core(self, observer, scheduler=None):
        return self.subject.subscribe(observer, scheduler)

    def connect(self, scheduler=None):
        """Connects the observable."""

        if not self.has_subscription:
            self.has_subscription = True

            def dispose():
                self.has_subscription = False

            disposable = self.source.subscribe(self.subject, scheduler)
            self.subscription = CompositeDisposable(disposable, Disposable.create(dispose))


        return self.subscription


class ConnectableBackpressureObservable(ConnectableObservableBase, BackpressureObservable):
    pass

class ConnectableSubFlowObservable(ConnectableObservableBase, SubFlowObservable):
    pass
