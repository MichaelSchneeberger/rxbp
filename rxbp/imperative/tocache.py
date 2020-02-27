from rxbp.flowable import Flowable
from rxbp.imperative.flowablecache import FlowableCache
from rxbp.scheduler import Scheduler


def to_cache(
        source: Flowable,
        scheduler: Scheduler = None,
):
    return FlowableCache(
        source=source,
        scheduler=scheduler,
    )
