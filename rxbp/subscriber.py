from rxbp.scheduler import Scheduler


class Subscriber:
    def __init__(self, scheduler: Scheduler, subscribe_scheduler: Scheduler):
        self.scheduler = scheduler
        self.subscribe_scheduler = subscribe_scheduler
