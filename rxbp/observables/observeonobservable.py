from rxbp.ack.ackimpl import Continue, Stop
from rxbp.ack.acksubject import AckSubject
from rxbp.observers.anonymousobserver import AnonymousObserver
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.observesubscription import ObserveSubscription
from rxbp.scheduler import Scheduler


class ObserveOnObservable(Observable):
    def __init__(self, source: Observable, scheduler: Scheduler):
        self.source = source
        self.scheduler = scheduler

    def observe(self, subscription: ObserveSubscription):
        observer = subscription.observer

        def on_next(v):
            ack_subject = AckSubject()

            def action(_, __):
                inner_ack = observer.on_next(v)

                if isinstance(inner_ack, Continue):
                    ack_subject.on_next(inner_ack)
                elif isinstance(inner_ack, Stop):
                    ack_subject.on_next(inner_ack)
                else:
                    inner_ack.subscribe(ack_subject)

            self.scheduler.schedule(action)
            return ack_subject

        def on_error(exc):
            def action(_, __):
                observer.on_error(exc)

            self.scheduler.schedule(action)

        def on_completed():
            def action(_, __):
                observer.on_completed()

            self.scheduler.schedule(action)

        observe_on_observer = AnonymousObserver(on_next_func=on_next, on_error_func=on_error,
                                                on_completed_func=on_completed)
        observer_on_subscription = ObserveSubscription(observe_on_observer, is_volatile=subscription.is_volatile)
        return self.source.observe(observer_on_subscription)
