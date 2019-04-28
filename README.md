
RxPy back-pressure extension
============================

An extension to the [RxPY](https://github.com/ReactiveX/RxPY) python 
library, that integrates back-pressure into observables.

The *rxbackpressure* library is inspired by [Monix](https://github.com/monix/monix), and has still experimental status. 

Example
-------

The syntax is similar to RxPY.

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

A rx Observable can be converted to a Flowable via the `from_rx` method.
Equivalently, a Flowable can be converted to a rx Observable via the `to_rx` method.

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



### zip operator

The `zip` operator has an optional `auto_match` argument that 

```python
import rxbp
from rxbp import op

rxbp.range_(10) \
    .map(lambda v: v+1) \
    .filter(lambda v: v%2==0) \
    .zip(rxbp.range_(10), auto_match=True) \
    .subscribe(print)
```

```
(2, 1)
(4, 3)
(6, 5)
(8, 7)
(10, 9)
```

### share operator

The `share` method does not return a multicast object. Instead, it takes a function exposing a
multicast Flowable.

```python
import rxbp
from rxbp import op

rxbp.range_(10) \
    .share(lambda f1: f1.zip(f1.map(lambda v: v+1).filter(lambda v: v%2==0))) \
    .subscribe(print)
```

```
(1, 2)
(3, 4)
(5, 6)
(7, 8)
(9, 10)
```

When to use an Observable, when Flowable?
-----------------------------------------



Implemented builders and operators
----------------------------------

### Create a Flowable

- `now` - creates a Flowable that emits a single element
- `from_` - create a Flowable that emits each element of an iterable
- `from_iterator` - see `from_`
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