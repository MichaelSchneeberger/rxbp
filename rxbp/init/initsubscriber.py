from rxbp.impl.subscriberimpl import SubscriberImpl
from rxbp.scheduler import Scheduler


def init_subscriber(
        scheduler: Scheduler,
        subscribe_scheduler: Scheduler,
):
    return SubscriberImpl(
        scheduler=scheduler,
        subscribe_scheduler=subscribe_scheduler,
    )
