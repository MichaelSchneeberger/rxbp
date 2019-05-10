
RxPy back-pressure extension
============================

An extension to the [RxPY](https://github.com/ReactiveX/RxPY) python 
library, that integrates back-pressure into observables.

The *rxbackpressure* library is inspired by [Monix](https://github.com/monix/monix), 
and has still experimental status. 

Installation
------------

rxbp v3.x runs on Python 3.6 or above. To install rxbp alpha version:

```
pip3 install --pre rxbp
```

Example
-------

The *rxbackpressure* syntax is similar to RxPY.

```python
# example taken from RxPY
import rxbp
from rxbp import op

source = rxbp.from_(["Alpha", "Beta", "Gamma", "Delta", "Epsilon"])

composed = source.pipe(
    op.map(lambda s: len(s)),
    op.filter(lambda i: i >= 5)
)
composed.subscribe(lambda value: print("Received {0}".format(value)))
```

Integrate RxPY
--------------

A RxPY Observable can be converted to a Flowable via the `from_rx` method.
Equivalently, a Flowable can be converted to a RxPY Observable via the `to_rx` method.

```python
import rx
import rxbp
from rxbp import op

rx_source = rx.of("Alpha", "Beta", "Gamma", "Delta", "Epsilon")

# convert Observable to Flowable
source = rxbp.from_rx(rx_source)

composed = source.pipe(
    op.map(lambda s: len(s)),
    op.filter(lambda i: i >= 5)
)

# convert Flowable to Observable
composed.to_rx().subscribe(lambda value: print("Received {0}".format(value)))
```

Differences from RxPY
---------------------

### Flowable

The Flowable is a data type for modeling and processing asynchronous and reactive 
streaming of events with non-blocking back-pressure. It is the equivalent of the 
RxPY Observable.

### share operator

The `share` method does not return a multicast object directly. Instead, it takes a function 
exposing a multicast Flowable via its arguments.

```python
import rxbp
from rxbp import op

rxbp.range(10).pipe(
    rxbp.op.share(lambda f1: f1.pipe(
        rxbp.op.zip(f1.pipe(
            rxbp.op.map(lambda v: v + 1),
            rxbp.op.filter(lambda v: v % 2 == 0)),
        )
    )),
).subscribe(print)
```
The previous code outputs:

```
(0, 2)
(1, 4)
(2, 6)
(3, 8)
(4, 10)
```

When to use an Flowable, when RxPY Observable?
-----------------------------------------

A Flowable is used when some asynchronous stages can't process the values 
fast enough and need a way to tell the upstream producer to slow down (called back-pressuring). But even 
if the generation of a values cannot be directly controlled, 
back-pressure can reduce the memory consumption by holding back the values in a buffer, and emitting them
in a synchronized fashion. Values from a Flowable need to be pulled by the downstream consumer,
which can lead to situations where the stream stops "flowing" due to bad coordination of back-pressuring
a mulicast Flowable.

An RxPY Observable on the other hand is push based. That means it emits elements as soon as
it receives them. 


Implemented builders and operators
----------------------------------

### Create a Flowable

- `return_value` - creates a Flowable that emits a single element
- `from_` - create a Flowable that emits each element of an iterable
- `from_iterator` - see `from_`
- `range` - creates a Flowable that emits elements defined by the range
- `from_rx` - creates a Flowable from a rx Observable that buffers each element emitted by the Observable

### Transforming operators

- `execute_on` - inject new scheduler that is used to subscribe Flowable
- `flat_map` - flattens a Flowable of Flowables
- `map` - applies a function to each element emitted by the Flowable
- `observe_on` - schedules element emitted by the Flowable on a dedicated scheduler
- `pairwise` - pairing two consecutive elements emitted by the Flowable
- `share` - exposes a multicast flowable that allows multible subscriptions
- `first` - emits the first element only
- `repeat_first` - repeat the first element by the Flowable forever (until disposed)
- `zip_with_index` - The same as `map`, except that the selector function takes 
index in addition to the value


### Combining operators

- `controlled_zip` - combines the elements emitted by two flowables 
into pairs in a controlled sequence. 
- `zip` - combines the elements emitted by two flowables into pairs in 
a strict sequence.

### Create a rx Observable

- `to_rx` - create a rx Observable from a Observable