import time

import rxbp
from rxbp.acknowledgement.continueack import continue_ack
from rxbp.schedulers.asyncioscheduler import AsyncIOScheduler
from rxbp.schedulers.eventloopscheduler import EventLoopScheduler
from rxbp.schedulers.threadpoolscheduler import ThreadPoolScheduler
from rxbp.testing.tobserver import TObserver


def work(o, skd):
    for i in range(10):
        o.on_next([i])
    o.on_completed()


source = rxbp.create(work)
source = source.pipe(
    rxbp.op.observe_on(scheduler=AsyncIOScheduler()),
    # rxbp.op.observe_on(scheduler=EventLoopScheduler()),
    # rxbp.op.observe_on(scheduler=ThreadPoolScheduler("receiver")),
)

sink = TObserver(immediate_continue=4)
source.subscribe(observer=sink, subscribe_scheduler=ThreadPoolScheduler("publisher"))

time.sleep(1)
assert sink.received == [0, 1, 2, 3, 4]

print("-" * 80)
sink.immediate_continue += 2
sink.ack.on_next(continue_ack)
time.sleep(1)
assert sink.received == [0, 1, 2, 3, 4, 5, 6, 7]

print("-" * 80)
sink.immediate_continue += 1
sink.ack.on_next(continue_ack)
time.sleep(1)
assert sink.received == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
