# Performing operations on hot observables with back-pressure results in better memory handling.
import datetime
import time

import rx

from rxbackpressure.observableop import ObservableOp
from rxbackpressure.schedulers.eventloopscheduler import EventLoopScheduler

scheduler1 = EventLoopScheduler()
scheduler2 = EventLoopScheduler()

n_samples = 10

def on_completed():
    done[0] = True

s1 = ObservableOp.from_(range(n_samples)).execute_on(scheduler1)
s2 = ObservableOp.from_(range(n_samples)).repeat_first().execute_on(scheduler2)

done = [False]

start_time = datetime.datetime.now()

# zip operation
s1.zip2(s2).subscribe_with(print, on_completed=on_completed)

while True:
    if done[0] is True:
        break
    time.sleep(0.5)

# print((datetime.datetime.now() - start_time).total_seconds())

# s1 = rx.Observable.from_(range(n_samples), scheduler=scheduler1)
# s2 = rx.Observable.from_(range(n_samples), scheduler=scheduler1)
#
# done = [False]
#
# start_time = datetime.datetime.now()
#
# s1.zip(s2, lambda *v: v).subscribe(on_completed=on_completed)
#
# while True:
#     if done[0] is True:
#         break
#     time.sleep(0.5)
#
# print((datetime.datetime.now() - start_time).total_seconds())
