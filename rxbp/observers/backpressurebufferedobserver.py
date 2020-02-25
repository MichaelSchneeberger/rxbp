import math
import threading
from queue import Queue
from typing import Optional

from rxbp.ack.acksubject import AckSubject
from rxbp.ack.continueack import ContinueAck, continue_ack
from rxbp.ack.operators.observeon import _observe_on
from rxbp.ack.single import Single
from rxbp.ack.stopack import StopAck, stop_ack
from rxbp.observer import Observer
from rxbp.scheduler import Scheduler
from rxbp.typing import ElementType


class BackpressureBufferedObserver(Observer):
    def __init__(
            self,
            underlying: Observer,
            scheduler: Scheduler,
            subscribe_scheduler: Scheduler,
            buffer_size: Optional[int],
    ):
        self.underlying = underlying
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler
        self.em = scheduler.get_execution_model()
        if buffer_size is None:
            self.buffer_size = math.inf
        else:
            self.buffer_size = buffer_size

        # states of buffered subscriber
        self.queue = Queue()
        self.last_iteration_ack = None
        self.upstream_is_complete = False
        self.downstream_is_complete = False
        self.items_to_push = 0
        self.back_pressured = None
        self.error_thrown = None

        self.lock = threading.RLock()

    def on_next(self, elem: ElementType):
        if self.upstream_is_complete or self.downstream_is_complete:
            return stop_ack
        else:
            with self.lock:
                is_back_pressured = self.back_pressured
                to_push = self.items_to_push
                self.items_to_push += 1

            if is_back_pressured is None:
                # buffer is not full, no back-pressure is needed
                if to_push < self.buffer_size:
                    self.queue.put(item=elem)
                    self.push_to_consumer(to_push)
                    return continue_ack

                # buffer is full, back-pressure is needed
                else:
                    ack = AckSubject()
                    self.back_pressured = ack
                    self.queue.put(item=elem)
                    self.push_to_consumer(to_push)
                    return ack

            else:
                self.queue.put(item=elem)
                self.push_to_consumer(to_push)
                return is_back_pressured

    def _on_completed_or_error(self, ex=None):
        if not self.upstream_is_complete and not self.downstream_is_complete:
            self.error_thrown = ex
            self.upstream_is_complete = True
            with self.lock:
                nr = self.items_to_push
                self.items_to_push += 1

            self.push_to_consumer(nr)

    def on_error(self, ex):
        self._on_completed_or_error(ex)

    def on_completed(self):
        self._on_completed_or_error(None)

    def push_to_consumer(self, current_nr: int):
        if current_nr == 0:
            def action(_, __):
                self.consumer_run_loop()

            self.scheduler.schedule(action)

    def consumer_run_loop(self):
        def signal_next(next):
            try:
                ack = self.underlying.on_next(next)
                return ack
            except Exception as exc:
                signal_error(exc)
                return stop_ack

        def signal_complete():
            # try:
            self.underlying.on_completed()
            # except:
            #     raise NotImplementedError

        def signal_error(ex):
            # try:
            self.underlying.on_error(ex)
            # except:
            #     raise NotImplementedError

        def go_async(next, next_size: int, ack: AckSubject, processed: int):
            def on_next(v):
                if isinstance(v, ContinueAck):
                    next_ack = signal_next(next)
                    is_sync = isinstance(ack, ContinueAck) or isinstance(ack, StopAck)
                    next_frame = self.em.next_frame_index(0) if is_sync else 0
                    fast_loop(next_ack, processed+next_size, next_frame)
                elif isinstance(v, StopAck):
                    self.downstream_is_complete = True

            class ResultSingle(Single):
                def on_next(self, elem):
                    on_next(elem)

                def on_error(self, exc: Exception):
                    raise NotImplementedError

            _observe_on(ack, self.scheduler).subscribe(ResultSingle())

        def fast_loop(prev_ack: AckSubject, last_processed:int, start_index: int):
            def stop_streaming():
                self.downstream_is_complete = True
                pass

            ack = continue_ack if prev_ack is None else prev_ack
            is_first_iteration = isinstance(ack, ContinueAck)
            processed = last_processed
            next_index = start_index

            while not self.downstream_is_complete:
                # fetch next
                try:
                    next = self.queue.get(block=False)
                    has_next = True
                except:
                    has_next = False
                # fetch size
                next_size = 1

                if has_next:
                    if next_index > 0 or is_first_iteration:
                        is_first_iteration = False

                        if isinstance(ack, ContinueAck):
                            ack = signal_next(next)
                            if isinstance(ack, StopAck):
                                self.downstream_is_complete = True
                                return
                            else:
                                is_sync = isinstance(ack, ContinueAck)
                                next_index = self.em.next_frame_index(next_index) if is_sync else 0
                                processed += next_size
                        elif isinstance(ack, StopAck):
                            stop_streaming()
                            return
                        else:
                            go_async(next, next_size, ack, processed)
                            return
                    else:
                        go_async(next, next_size, ack, processed)
                        return
                elif self.upstream_is_complete:
                    stop_streaming()
                    with self.lock:
                        self.items_to_push -= (processed + 1)

                    if self.error_thrown is None:
                        signal_complete()
                    else:
                        signal_error(self.error_thrown)
                    return
                else:
                    self.last_iteration_ack = ack
                    with self.lock:
                        self.items_to_push -= 1
                        remaining = self.items_to_push
                    processed = 0

                    if remaining <= 0:
                        with self.lock:
                            bp = self.back_pressured
                            self.back_pressured = None
                        if bp is not None:
                            bp.on_next(continue_ack)
                        return

        # try:
        fast_loop(prev_ack=self.last_iteration_ack, last_processed=0, start_index=0)
        # except:
        #     raise NotImplementedError


