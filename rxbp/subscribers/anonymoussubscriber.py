from typing import Callable, Dict, Any, Optional

from rx.disposable import Disposable

from rxbp.observable import Observable
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber


class AnonymousSubscriber(Subscriber):
    def __init__(self, scheduler: Scheduler, subscribe_scheduler: Scheduler):
        super().__init__(scheduler=scheduler, subscribe_scheduler=subscribe_scheduler)
