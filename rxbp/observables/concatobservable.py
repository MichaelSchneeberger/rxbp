from typing import List, Iterator, Iterable

from rx.disposable import CompositeDisposable, SerialDisposable, SingleAssignmentDisposable, Disposable

from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observesubscription import ObserveSubscription
from rxbp.scheduler import Scheduler


class ConcatObservable(Observable):
    def __init__(self, sources: Iterable[Observable], subscribe_scheduler: Scheduler):
        super().__init__()

        self._sources = iter(sources)
        self._subscribe_scheduler = subscribe_scheduler

    def observe(self, subscription: ObserveSubscription):
        observer = subscription.observer
        source = self

        subscription_disposable = SerialDisposable()
        outer_subscription = SerialDisposable()
        inner_subscription = SerialDisposable()     # probably not necessary

        class ConcatObserver(Observer):
            def __init__(self):
                self.ack = None

            def on_next(self, v):
                self.ack = observer.on_next(v)
                return self.ack

            def on_error(self, exc):
                return observer.on_error(exc)

            def on_completed(self):
                try:
                    next_source = next(source._sources)
                    has_element = True
                except StopIteration:
                    has_element = False

                if has_element:
                    def observe_next():
                        def action(_, __):
                            next_subscription = ObserveSubscription(ConcatObserver(), is_volatile=subscription.is_volatile)
                            disposable = next_source.observe(next_subscription)
                            inner_subscription.disposable = disposable

                        disposable = source._subscribe_scheduler.schedule(action)
                        subscription_disposable.disposable = disposable

                    if self.ack:
                        disposable = self.ack.subscribe(lambda _: observe_next(), scheduler=source._subscribe_scheduler)
                        outer_subscription.disposable = disposable
                    else:
                        observe_next()
                else:
                    observer.on_completed()

        concat_observer = ConcatObserver()
        concat_subscription = ObserveSubscription(concat_observer, is_volatile=subscription.is_volatile)

        first_source = next(self._sources)
        disposable = first_source.observe(concat_subscription)

        return CompositeDisposable(disposable, subscription_disposable, outer_subscription, inner_subscription)
