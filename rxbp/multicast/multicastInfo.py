from rxbp.scheduler import Scheduler


class MultiCastInfo:
    def __init__(self, source_scheduler: Scheduler, multicast_scheduler: Scheduler):
        self.source_scheduler = source_scheduler
        self.multicast_scheduler = multicast_scheduler
