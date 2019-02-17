from optparse import Option
from queue import Queue

from rx import config

from rxbp.ack import Stop, Continue, Ack, continue_ack
from rxbp.observer import Observer
from rxbp.scheduler import SchedulerBase


class BufferedSubscriber(Observer):
    def __init__(self, observer: Observer, scheduler: SchedulerBase, buffer_size: int):
        self.observer = observer
        self.scheduler = scheduler
        self.em = scheduler.get_execution_model()
        self.buffer_size = buffer_size

        self.queue = Queue()

        self.last_iteration_ack = None

        self.upstream_is_complete = False
        self.downstream_is_complete = False

        self.items_to_push = 0
        self.back_pressured = None
        self.error_thrown = None

        self.lock = config['concurrency'].RLock()

    def push_on_next(self, elem, last_to_push: int = None):
        if self.upstream_is_complete or self.downstream_is_complete:
            return Stop()
        else:
            if last_to_push is None:
                with self.lock:
                    to_push =  self.items_to_push
                    self.items_to_push += 1
            else:
                to_push = last_to_push

            if self.back_pressured is None:
                if to_push < self.buffer_size:
                    self.queue.put(item=elem)
                    self.push_to_consumer(to_push)
                    return continue_ack
                else:
                    ack = Ack()
                    with self.lock:
                        self.back_pressured = ack
                    self.queue.put(item=elem)
                    self.push_to_consumer(to_push)
                    return ack
            else:
                self.queue.put(item=elem)
                self.push_to_consumer(to_push)
                return self.back_pressured

    def on_next(self, v):
        return self.push_on_next(v, None)

    def push_complete(self, ex=None, to_push: int = None):
        if not self.upstream_is_complete and not self.downstream_is_complete:
            self.error_thrown = ex
            self.upstream_is_complete = True
            if to_push is None:
                with self.lock:
                    nr = self.items_to_push
                    self.items_to_push += 1
            else:
                nr = to_push

            self.push_to_consumer(nr)

    def on_error(self, ex):
        self.push_complete(ex, None)

    def on_completed(self):
        self.push_complete(None, None)

    def push_to_consumer(self, current_nr: int):
        if current_nr == 0:
            def action(_, __):
                self.consumer_run_loop()

            self.scheduler.schedule(action)

    def consumer_run_loop(self):
        def signal_next(next):
            try:
                ack = self.observer.on_next(next)
                return ack
            except:
                raise NotImplementedError

        def signal_complete():
            try:
                self.observer.on_completed()
            except:
                raise NotImplementedError

        def signal_error(ex):
            try:
                self.observer.on_error(ex)
            except:
                raise NotImplementedError

        def go_async(next, next_size: int, ack: Ack, processed: int):
            def on_next(v):
                if isinstance(v, Continue):
                    next_ack = signal_next(next)
                    is_sync = isinstance(ack, Continue) or isinstance(ack, Stop)
                    next_frame = self.em.next_frame_index(0) if is_sync else 0
                    fast_loop(next_ack, processed+next_size, next_frame)
                elif isinstance(v, Stop):
                    self.downstream_is_complete = True

            ack.observe_on(self.scheduler).subscribe(on_next=on_next)

        def fast_loop(prev_ack: Ack, last_processed:int, start_index: int):
            def stop_streaming():
                self.downstream_is_complete = True
                pass

            ack = continue_ack if prev_ack is None else prev_ack
            is_first_iteration = isinstance(ack, Continue)
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

                        if isinstance(ack, Continue):
                            ack = signal_next(next)
                            if isinstance(ack, Stop):
                                self.downstream_is_complete = True
                                return
                            else:
                                is_sync = isinstance(ack, Continue)
                                next_index = self.em.next_frame_index(next_index) if is_sync else 0
                                processed += next_size
                        elif isinstance(ack, Stop):
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

                    if remaining == 0:
                        with self.lock:
                            bp = self.back_pressured
                            self.back_pressured = None
                        if bp is not None:
                            bp.on_next(continue_ack)
                            bp.on_completed()
                        return

        try:
            fast_loop(prev_ack=self.last_iteration_ack, last_processed=0, start_index=0)
        except:
            raise NotImplementedError


