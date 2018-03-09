from rx.concurrency import current_thread_scheduler
from rx.core import Disposable


class BackpressureGreadily:
    @staticmethod
    def apply(backpressure, scheduler=None):
        scheduler = scheduler or current_thread_scheduler

        def scheduled_action(a, s):
            def handle_msg(num_of_items):
                # print('handle_msg %s' % num_of_items)
                if num_of_items > 0:
                    scheduler.schedule(scheduled_action)

            backpressure.request(1).subscribe(handle_msg)

        scheduler.schedule(scheduled_action)

        def dispose():
            print('dispsed!')

        return Disposable.create(dispose)
