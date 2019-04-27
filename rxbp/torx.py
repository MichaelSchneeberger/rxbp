from rx.core import typing
from rx.core.abc import Observable

from rxbp.ack import Continue
from rxbp.flowablebase import FlowableBase
from rxbp.observer import Observer
from rxbp.schedulers.currentthreadscheduler import current_thread_scheduler
from rxbp.subscriber import Subscriber


def to_rx(source: FlowableBase):
    """ Converts this observable to an rx.Observable

    :param scheduler:
    :return:
    """

    class FromFlowableObservable(Observable):
        def _subscribe_core(self, observer: typing.Observer, scheduler: typing.Scheduler = None):
            class ToRxObserver(Observer):
                def on_next(self, v):
                    for e in v():
                        observer.on_next(e)
                    return Continue()

                def on_error(self, err):
                    observer.on_error(err)

                def on_completed(self):
                    observer.on_completed()

            scheduler_ = scheduler or current_thread_scheduler
            subscriber = Subscriber(scheduler=scheduler_, subscribe_scheduler=current_thread_scheduler)
            return source.subscribe_(observer=ToRxObserver(), subscriber=subscriber)

    return FromFlowableObservable()