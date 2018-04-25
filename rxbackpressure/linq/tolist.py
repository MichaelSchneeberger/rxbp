from rx import Observable, AnonymousObservable, config
from rx.internal import extensionmethod
from rx.subjects import Subject, AsyncSubject

from rxbackpressure.backpressuretypes.backpressuregreadily import BackpressureGreadily
from rxbackpressure.backpressuretypes.stoprequest import StopRequest
from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.backpressurebase import BackpressureBase
from rxbackpressure.core.backpressureobservable import BackpressureObservable


@extensionmethod(BackpressureObservable)
def to_list(self):

    class ToListBackpressure(BackpressureBase):
        def __init__(self, parent_backpressure, scheduler):
            self.parent_backpressure = parent_backpressure
            self.scheduler = scheduler
            self.done = False

            self.lock = config["concurrency"].RLock()

        def request(self, number_of_items):
            if isinstance(number_of_items, StopRequest):
                return self.parent_backpressure.request(number_of_items)

            is_done = True
            with self.lock:
                if not self.done:
                    is_done = False
                    self.done = True

            if not is_done:
                def scheduled_action(a, s):
                    def handle_msg(num_of_items):
                        # print('handle_msg %s' % num_of_items)
                        if num_of_items > 0:
                            self.scheduler.schedule(scheduled_action)
                        else:
                            subject.on_next(1)
                            subject.on_completed()

                    self.parent_backpressure.request(1).subscribe(handle_msg)

                subject = Subject()
                self.scheduler.schedule(scheduled_action)
                return subject
            else:
                subject = AsyncSubject()
                subject.on_next(0)
                subject.on_completed()
                return subject

    def subscribe_func(observer, scheduler):
        parent_scheduler = scheduler

        def subscribe_bp(backpressure):
            tolist_backpressure = ToListBackpressure(backpressure, scheduler=parent_scheduler)
            return observer.subscribe_backpressure(tolist_backpressure)

        def subscribe_func(o1):
            disposable = self.subscribe(observer=o1, subscribe_bp=subscribe_bp,
                           scheduler=parent_scheduler)
            return disposable

        obs1 = AnonymousObservable(subscribe=subscribe_func)
        disposable = obs1.to_list().subscribe(observer)
        # print(disposable)
        return disposable

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func, name='to_list')
