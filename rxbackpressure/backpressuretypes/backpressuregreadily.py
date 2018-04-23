from rx.concurrency import current_thread_scheduler, immediate_scheduler
from rx.core import Disposable


class BackpressureGreadily:
    @staticmethod
    def apply(backpressure, scheduler2=None):
        # scheduler = scheduler or current_thread_scheduler

        def scheduled_action(a, s):
            def handle_msg(num_of_items):
                # print('handle_msg %s' % num_of_items)
                if num_of_items > 0:
                    immediate_scheduler.schedule(scheduled_action)

            subject = backpressure.request(1)
            subject.subscribe(handle_msg)

        immediate_scheduler.schedule(scheduled_action)

        def dispose():
            #     print('dispsed!')
            pass

        return Disposable.create(dispose)
