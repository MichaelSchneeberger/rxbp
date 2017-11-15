
RxPy back-pressure extension
===========================

An extension to the [RxPY](https://github.com/ReactiveX/RxPY) python library, that takes the idea of back pressure a bit further.

### Creating backpressured observables

- `to_backpressure` - create a BackpressureObservable from a rx Observable
- `repeat_first` - create a BackpressureObservable from a rx Observable by repeating the first item.
- `to_observable` - create a rx Observable from a BackpressureObservable


### Transforming backpressured observables

- `flat_map` - flatten inner BackpressureObservables emissioned by the outer BackpressureObservable into a single BackpressureObservable
- `map` - transform the items emitted by an BackpressureObservable by applying a function to each item
- `pairwise` - pairing two consecutive items emitted by an BackpressureObservable


### Filtering back-pressured observables

- `first` - emit only the first item, or the first item that meets a condition, from an BackpressureObservable


### Combining back-pressured observables

- `window` - for each item from one BackpressureObservable, subdivide items from another BackpressureObservable via is_lower and is_higher functions into BackpressureObservable windows and emit these windows rather than emitting the items one at a time
- `zip` - combine the emissions of multiple BackpressureObservables together via a specified function and emit single items for each combination based on the results of this function



Back-pressure vs. no back-pressure
----------------------------------

[RxPY](https://github.com/ReactiveX/RxPY) offers a back-pressure 
observable called ControlledSubject, which implements a method 
request(number). Only after requesting a number of items, the 
ControlledSubject will emit them.

The equivalent Subject in rxbackpressure is also called ControlledSubject. 
The back-pressure logic, however, is integrated into the back-pressure 
observable. It is therefore possible to construct a back-pressure 
observable sequence without having to touch the back-pressure part. 

The operators `to_backpressure` and `to_observable` let you change 
easily from back-pressured observables to non-back-pressured observables. 
Depending on the task, one might be more appropriate than the other. 
The `to_observable` will greadily request for more items as soon as 
it got them.

# time synchronization example

A recorded time-value-pair sequence needs to be synchronized 
to two different timebases. We might have recorded it from a device, 
which is not perfectly synchronized to two other devices.

The time-value-pair sequence is emitted by a Subject called 
`time_value_record`. The two sychronization Subject `sync1` 
and `sync2` emit just one item, which is the time correction. 

```python
from rx.subjects import Subject

# time value samples recorded by some device
time_value_record = Subject()

# separate time and value
time = time_value_record.map(lambda pair: pair[0])
signal = time_value_record.map(lambda pair: pair[1])

# timebase synchronization
sync1 = Subject()
sync2 = Subject()
```

As we don't know when the two synchronization items get emitted, 
we need to buffer the time sequence with the `to_list` operator.

```python
# rxpy implementation

from rx import Observable

def synchronize_time_samples(time: List[float], sync_time: float):
    return Observable.from_(time).map(lambda t: t + sync_time)

# synchronize time samples to two timebases
time_sync1 = time.to_list().zip(sync1, synchronize_time_samples).flat_map(lambda v: v)
time_sync2 = time.to_list().zip(sync2, synchronize_time_samples).flat_map(lambda v: v)

# pairing time value observables
time_sync1.zip(signal, lambda t, v: (t, v)).subscribe(print)
time_sync2.zip(signal, lambda t, v: (t, v)).subscribe(print)
```

Now, let us implement this with rxbackpressure observables. 
As the back-pressure takes care of buffering,
the implementation becomes rather easy.
First, we turn the observable to a BackpressureObservable 
via the `to_backpressure` and `repeat_first` operator. Then we zip the
time observable with the first and second tim esychronization observable 
respectively.

```python
# rxbackpressure implementation

# synchronize time samples to two timebases
time_sync2_bp = time.to_backpressure().zip(sync2.repeat_first(), lambda t, sync_time: t + sync_time)
time_sync1_bp = time.to_backpressure().zip(sync1.repeat_first(), lambda t, sync_time: t + sync_time)

# pairing time value observables
time_sync1_bp.to_observable().zip(signal, lambda t, v: (t, v)).subscribe(print)
time_sync2_bp.to_observable().zip(signal, lambda t, v: (t, v)).subscribe(print)
```

Let's apply some items to the Subjects.

```python
# emulating hot observable
Observable.range(0,100) \
    .map(lambda v: (float(v)+3.2)/1000) \
    .map(lambda t: (t, math.sin(t/0.05*2*math.pi))) \
    .subscribe(time_value_record)
Observable.just(-3.2/1000).subscribe(sync1)
Observable.just(2/1000).subscribe(sync2)
```