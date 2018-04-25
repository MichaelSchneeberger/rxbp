import math

from rx import Observable
from rx.subjects import Subject

# import needed for to_backpressure operator
import rxbackpressure

# time value samples recorded by some device
time_value_record = Subject()

# separate time and value
time = time_value_record.map(lambda pair: pair[0])
signal = time_value_record.map(lambda pair: pair[1])

# timebase synchronization
sync1 = Subject()
sync2 = Subject()

# synchronize time samples to two timebases
time_sync2_bp = time.to_backpressure().zip(sync2.repeat_first(), lambda t, sync_time: t + sync_time)
time_sync1_bp = time.to_backpressure().zip(sync1.repeat_first(), lambda t, sync_time: t + sync_time)

# pairing time value observables
time_sync1_bp.to_observable().zip(signal, lambda t, v: (t, v)).subscribe()
time_sync2_bp.to_observable().zip(signal, lambda t, v: (t, v)).subscribe(print, on_completed=lambda: print('completed'))

# emulating hot observable
Observable.range(0,100) \
    .map(lambda v: (float(v)+3.2)/1000) \
    .map(lambda t: (t, math.sin(t/0.05*2*math.pi))) \
    .subscribe(time_value_record)
Observable.just(-3.2/1000).subscribe(sync1)
Observable.just(2/1000).subscribe(sync2)
