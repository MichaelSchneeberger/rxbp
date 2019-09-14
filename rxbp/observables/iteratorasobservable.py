from typing import Iterator, Any, List, Generator, Callable

import rx
from rx.disposable import Disposable, BooleanDisposable, CompositeDisposable
from rxbp.ack.ackimpl import Continue, Stop
from rxbp.ack.observeon import _observe_on
from rxbp.ack.single import Single

from rxbp.observable import Observable
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.observer import Observer
from rxbp.observerinfo import ObserverInfo
from rxbp.scheduler import SchedulerBase, ExecutionModel, Scheduler
from rxbp.selectors.selectionmsg import select_next, select_completed


class IteratorAsObservable(Observable):
    def __init__(
            self,
            iterator: Generator[Callable[[], Generator[Any, Any, None]], Any, None],
            scheduler: Scheduler,
            subscribe_scheduler: Scheduler,
            on_finish: Disposable = Disposable(),
    ):
        super().__init__()

        self.iterator = iterator
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler
        self.on_finish = on_finish
        # self.selector = ObservablePublishSubject(scheduler=scheduler)

    def observe(self, observer_info: ObserverInfo):

        observer = observer_info.observer

        # try:
        # if not has_next:
        #     observer.on_completed()
        #     return Disposable()
        # else:
        d1 = BooleanDisposable()

        def action(_, __):
            try:
                item = next(self.iterator)
                has_next = True
            except StopIteration:
                has_next = False
            except Exception as e:
                # stream errors
                observer.on_error(e)
                return Disposable()

            if not has_next:
                observer.on_completed()
            else:
                # start sending items
                self.fast_loop(item, observer, self.scheduler, d1, self.scheduler.get_execution_model(),
                               sync_index=0)

        d2 = self.subscribe_scheduler.schedule(action)
        return CompositeDisposable(d1, d2)
        # except:
        #     raise Exception('fatal error')

    def trigger_cancel(self, scheduler: SchedulerBase):
        try:
            self.on_finish.dispose()
        except Exception as e:
            scheduler.report_failure(e)

    def reschedule(self, ack, next_item, observer, scheduler: SchedulerBase, disposable, em: ExecutionModel):
        class ResultSingle(Single):
            def on_next(_, next):
                if isinstance(next, Continue):
                    try:
                        self.fast_loop(next_item, observer, scheduler, disposable, em, sync_index=0)
                    except Exception as e:
                        self.trigger_cancel(scheduler)
                        scheduler.report_failure(e)
                else:
                    self.trigger_cancel(scheduler)

            def on_error(_, err):
                self.trigger_cancel(scheduler)
                scheduler.report_failure(err)

        _observe_on(source=ack, scheduler=scheduler).subscribe(ResultSingle())
        # ack.subscribe(ResultSingle())

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

                # def gen_select_msg():
                #     for _ in range(len(current_item)):
                #         yield select_next
                #     yield select_completed
                #
                # self.selector.on_next(gen_select_msg)

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
