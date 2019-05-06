from typing import Iterator, Any, List

import rx
from rx.disposable import Disposable, BooleanDisposable

from rxbp.ack import Continue, Stop
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.scheduler import SchedulerBase, ExecutionModel, Scheduler


class IteratorAsObservable(Observable):
    def __init__(self, iterator: Iterator, scheduler: Scheduler, subscribe_scheduler: Scheduler,
                 on_finish: Disposable = Disposable()):
        super().__init__()

        self.iterator = iterator
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler
        self.on_finish = on_finish

    def observe(self, observer: Observer):

        try:
            item = next(self.iterator)
            has_next = True
        except StopIteration:
            has_next = False
        except Exception as e:
            # stream errors
            observer.on_error(e)
            return Disposable()

        try:
            if not has_next:
                observer.on_completed()
                return Disposable()
            else:
                disposable = BooleanDisposable()

                def action(_, __):
                    # start sending items
                    self.fast_loop(item, observer, self.scheduler, disposable, self.scheduler.get_execution_model(),
                                   sync_index=0)

                self.subscribe_scheduler.schedule(action)
                return disposable
        except:
            raise Exception('fatal error')

    def trigger_cancel(self, scheduler: SchedulerBase):
        try:
            self.on_finish.dispose()
        except Exception as e:
            scheduler.report_failure(e)

    def reschedule(self, ack, next_item, observer, scheduler: SchedulerBase, disposable, em: ExecutionModel):
        def on_next(next):
            if isinstance(next, Continue):
                try:
                    self.fast_loop(next_item, observer, scheduler, disposable, em, sync_index=0)
                except Exception as e:
                    self.trigger_cancel(scheduler)
                    scheduler.report_failure(e)
            else:
                self.trigger_cancel(scheduler)

        def on_error(err):
            self.trigger_cancel(scheduler)
            scheduler.report_failure(err)

        ack.pipe(rx.operators.observe_on(scheduler)).subscribe(on_next=on_next, on_error=on_error)

    def fast_loop(self, current_item, observer, scheduler: Scheduler,
                  disposable: BooleanDisposable, em: ExecutionModel, sync_index: int):
        while True:
            try:
                next_item = next(self.iterator)
                has_next = True
            except StopIteration:
                has_next = False
                next_item = None
            except Exception as e:
                # stream errors == True
                self.trigger_cancel(scheduler)

                if not disposable.is_disposed:
                    observer.on_error(e)
                else:
                    scheduler.report_failure(e)

                has_next = False
                next_item = None

            try:
                ack = observer.on_next(current_item)

                if not has_next:
                    try:
                        self.on_finish.dispose()
                    except Exception as e:
                        observer.on_error(e)
                    else:
                        observer.on_completed()
                    break
                else:
                    if isinstance(ack, Continue):
                        next_index = em.next_frame_index(sync_index)
                    elif isinstance(ack, Stop):
                        next_index = -1
                    else:
                        next_index = 0

                    if next_index > 0:
                        current_item = next_item
                        sync_index = next_index
                    elif next_index == 0 and not disposable.is_disposed:
                        self.reschedule(ack, next_item, observer, scheduler, disposable, em)
                        break
                    else:
                        self.trigger_cancel(scheduler)
                        break
            except:
                raise Exception('fatal error')
