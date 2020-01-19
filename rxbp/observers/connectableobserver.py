from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.ack.operators.map import _map
from rxbp.ack.operators.mergeall import _merge_all
from rxbp.ack.single import Single
from rxbp.ack.stopack import StopAck
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


class ConnectableObserver(Observer):
    def __init__(self, underlying: Observer, scheduler: Scheduler, subscribe_scheduler: Scheduler,
                 is_volatile: bool = None):
        self.underlying = underlying
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler
        self.is_volatile = is_volatile or False

        self.root_ack = AckSubject()
        self.connected_ack = self.root_ack
        # self.connected_ref = None
        self.is_connected = False
        # self.is_connected_started = False
        # self.scheduled_done = False
        # self.schedule_error = None
        self.was_canceled = False
        # self.queue = Queue()
        # self.lock = threading.RLock()

    def connect(self):
        self.is_connected = True
        self.root_ack.on_next(continue_ack)

    # def connect(self):
    #     with self.lock:
    #         if not self.is_connected and not self.is_connected_started:
    #             self.is_connected_started = True
    #
    #             buffer_was_drained = AckSubject()
    #
    #             class ResultSingle(Single):
    #                 def on_error(self, exc: Exception):
    #                     raise NotImplementedError
    #
    #                 def on_next(_, response):
    #                     if isinstance(response, Continue):
    #                         self.root_ack.on_next(continue_ack)
    #                         source.is_connected = True
    #                         source.queue = None
    #                         # source.connected_ack = None
    #                         # todo: fill in
    #                     elif isinstance(response, Stop):
    #                         raise NotImplementedError
    #                     else:
    #                         raise Exception(f'illegal acknowledgment value "{response}"')
    #
    #             buffer_was_drained.subscribe(ResultSingle())
    #
    #             source = self
    #
    #             class InnerConnectableObserver(Observer):
    #                 def __init__(self):
    #                     self.ack: Optional[AckSubject] = None
    #
    #                 def on_next(self, v):
    #                     ack = source.underlying.on_next(v)
    #                     self.ack = ack
    #
    #                     class ResultSingle(Single):
    #                         def on_error(self, exc: Exception):
    #                             raise NotImplementedError
    #
    #                         def on_next(_, v):
    #                             if isinstance(v, Stop):
    #                                 buffer_was_drained.on_next(v)
    #
    #                     ack.subscribe(ResultSingle())
    #
    #                     return ack
    #
    #                 def on_error(self, err):
    #                     raise NotImplementedError
    #
    #                 def on_completed(self):
    #                     if not source.scheduled_done:
    #                         if self.ack is None:
    #                             buffer_was_drained.on_next(continue_ack)
    #                         else:
    #                             class ResultSingle(Single):
    #                                 def on_error(self, exc: Exception):
    #                                     raise NotImplementedError
    #
    #                                 def on_next(_, v):
    #                                     if isinstance(v, Continue):
    #                                         buffer_was_drained.on_next(continue_ack)
    #
    #                             self.ack.subscribe(ResultSingle())
    #                     elif source.schedule_error is not None:
    #                         raise NotImplementedError
    #                     else:
    #                         source.underlying.on_completed()
    #
    #             class EmptyObject:
    #                 pass
    #
    #             self.queue.put(EmptyObject)
    #             subscription = ObserverInfo(observer=InnerConnectableObserver(), is_volatile=self.is_volatile)
    #             disposable = IteratorAsObservable(iter(self.queue.get, EmptyObject), scheduler=self.scheduler,
    #                                               subscribe_scheduler=self.subscribe_scheduler) \
    #                 .observe(subscription)
    #
    #             self.connected_ref = buffer_was_drained, disposable
    #     return self.connected_ref

    # def push_first(self, elem):
    #     with self.lock:
    #         if self.is_connected or self.is_connected_started:
    #             throw_exception = True
    #         elif not self.scheduled_done:
    #             throw_exception = False
    #             self.queue.put(elem, block=False)
    #         else:
    #             throw_exception = False
    #
    #     if throw_exception:
    #         raise Exception('Observer was already connected, so cannot pushFirst')

    # def push_first_all(self, cs: Iterable):
    #     with self.lock:
    #         if self.is_connected or self.is_connected_started:
    #             throw_exception = True
    #         elif not self.scheduled_done:
    #             throw_exception = False
    #             for elem in cs:
    #                 self.queue.put(elem, block=False)
    #         else:
    #             throw_exception = False
    #
    #     if throw_exception:
    #         raise Exception('Observer was already connected, so cannot pushFirst')

    # def push_complete(self):
    #     with self.lock:
    #         if self.is_connected or self.is_connected_started:
    #             throw_exception = True
    #         elif not self.scheduled_done:
    #             throw_exception = False
    #             self.scheduled_done = True
    #         else:
    #             throw_exception = False
    #
    #     if throw_exception:
    #         raise Exception('Observer was already connected, so cannot pushFirst')

    # def push_error(self, ex: Exception):
    #     with self.lock:
    #         if self.is_connected or self.is_connected_started:
    #             throw_exception = True
    #         elif not self.scheduled_done:
    #             throw_exception = False
    #             self.scheduled_done = True
    #             self.schedule_error = ex
    #         else:
    #             throw_exception = False
    #
    #     if throw_exception:
    #         raise Exception('Observer was already connected, so cannot pushFirst')

    def on_next(self, elem: ElementType):
        if not self.is_connected:
            def __(v):
                if isinstance(v, ContinueAck):
                    ack = self.underlying.on_next(elem)
                    return ack
                else:
                    return StopAck()

            new_ack = AckSubject()
            # _merge_all(_map(_observe_on(self.connected_ack, scheduler=self.scheduler), func=__)).subscribe(new_ack)
            _merge_all(_map(self.connected_ack, func=__)).subscribe(new_ack)

            self.connected_ack = new_ack
            return self.connected_ack
        elif not self.was_canceled:
            ack = self.underlying.on_next(elem)
            return ack
        else:
            return StopAck()

    def on_error(self, err):
        self.was_canceled = True

        class ResultSingle(Single):
            def on_error(self, exc: Exception):
                raise NotImplementedError

            def on_next(_, v):
                if isinstance(v, ContinueAck):
                    self.underlying.on_error(err)

        # _observe_on(self.connected_ack, scheduler=self.scheduler).subscribe(ResultSingle())
        self.connected_ack.subscribe(ResultSingle())

    def on_completed(self):
        self.was_canceled = True

        class ResultSingle(Single):
            def on_error(self, exc: Exception):
                raise NotImplementedError

            def on_next(_, v):
                if isinstance(v, ContinueAck):
                    self.underlying.on_completed()

        # _observe_on(self.connected_ack, scheduler=self.scheduler).subscribe(ResultSingle())
        self.connected_ack.subscribe(ResultSingle())

    def dispose(self):
        self.was_canceled = True
        self.is_connected = True
