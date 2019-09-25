
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

*rxbackpressure* has a similar syntax like RxPY.

```python
# example taken from RxPY
import rxbp

source = rxbp.from_(["Alpha", "Beta", "Gamma", "Delta", "Epsilon"])

composed = source.pipe(
    rxbp.op.map(lambda s: len(s)),
    rxbp.op.filter(lambda i: i >= 5)
)
composed.subscribe(lambda value: print("Received {0}".format(value)))
```

Integrate RxPY
--------------

A RxPY Observable can be converted to a `Flowable` by using the `from_rx` method.
Equivalently, a `Flowable` can be converted to a RxPY Observable 
by using the `to_rx` method.

```python
import rx
import rxbp

rx_source = rx.of("Alpha", "Beta", "Gamma", "Delta", "Epsilon")

# convert Observable to Flowable
source = rxbp.from_rx(rx_source)

composed = source.pipe(
    rxbp.op.map(lambda s: len(s)),
    rxbp.op.filter(lambda i: i >= 5)
)

# convert Flowable to Observable
composed.to_rx().subscribe(lambda value: print("Received {0}".format(value)))
```

Differences from RxPY
---------------------

### Flowable

Similar to a RxPY Observable, a `Flowable` implements a `subscribe` method, which
makes it possible to describe a data flow from source to sink. The description is
done with *rxbackpressure* operators exposed by `rxbp.op`.

Like in
functional programming, usings *rxbackpressure* operators does not create
any mutable states but rather concatenates functions without calling them
yet. Or in other words, we first describe what we want to do, and then 
we execute the plan. A `Flowable` is executed by calling its `subscribe`
method. This will then start a chain reaction where downsream `Flowables`
call the `subscribe` method of their linked upstream `Flowable` until
the sources start emitting data. Once a `Flowable` is subscribed, we
allow it to have mutable states where it make sense.
 
Compared to RxPY Observables, a `Flowable` uses `Observers` that are
able to back-pressure an `on_next` method call.

### MultiCast (experimental)

A `MultiCast` is used when a `Flowable` emits elements to more than
one `Observer`, and in case of nested Flowables like 
`Flowable[Flowable]`.

In RxPY, there are operators called `publish` and `share`,
which create a multicast observable that can then be subscribed
by more than one downstream subscriber. In *rxbackpressure*, however,
there is no such operator, and there are good reasons to not have one.
The problem is that an RxPY observable sequence using a multicast 
observable can only be subscribed once. This is because calling
the `subscribe` method of a multicast observable more than once will
not rerun the observable but register the subscriber to a multicast
subscription.

``` python
import rx
from rx import operators as op

o = rx.range(4).pipe(
    op.share(),
)

o.subscribe(print)
o.subscribe(print)
```

The previous code outputs:

```
0
1
2
3
```

To get rid of this drawback, *rxbackpressure* introduces the `MultiCast`.
A `MultiCast` represents a collection of `Flowable` and can
 be though of as `Flowable[T[Flowable]]` where T is defined by the user.
It provide operators `rxbp.multicast.op` to share `Flowables` but also
work with nested `Flowables` in a safe way.

```python
import rxbp

f = rxbp.to_multicast(rxbp.range(10)).pipe(
    rxbp.multicast.op.share(lambda base: base[0].pipe(
        rxbp.op.zip(base[0].pipe(
            rxbp.op.map(lambda v: v + 1),
            rxbp.op.filter(lambda v: v % 2 == 0)),
        ),
    )),
).to_flowable()
f.subscribe(print)
```

The previous code outputs:

```
(0, 2)
(1, 4)
(2, 6)
(3, 8)
(4, 10)
```


### match operator (experimental)

The `match` operator tries to match two `Flowable`, 
otherwise it raises an exception.
Two observables match if they have the same base or if there exists 
a mapping that maps 
one base to the base of the other `Flowable`. These mappings 
are called "selectors" and propagated internally when subscribing
to a `Flowable`.

If two Flowables have the same base, 
they should match in the sense of the `zip` operator,
e.g. every pair of elements that get zipped from the two
 Flowables should belong together.

```python
import rxbp
from rxbp import op

rxbp.range(10).pipe(
    rxbp.op.share(lambda f1: f1.pipe(
        rxbp.op.match(f1.pipe(
            rxbp.op.filter(lambda v: v % 2 == 0)),
        )
    )),
).subscribe(print)
```

The previous code outputs:

```
(1, 1)
(3, 3)
(5, 5)
(7, 7)
(9, 9)
```

When to use an Flowable, when RxPY Observable?
-----------------------------------------

A `Flowable` is used when some asynchronous stage cannot process the
data fast enough or needs to synchronize the data with some other event.
Let's take the `zip` operator for instance. It gets elements from
two or more sources and emits a tuple once it received one
element from each source. But what happens if one source emits the
elements before the others do? Without back-pressure, the `zip` operation
has to buffer the elements until it receives data from the other sources.
This might be ok depending on how much data needs to be buffered. But
often we can not risk having too much data buffered somewhere in our
stream, which might cause a out of memory exception. Therefore, it
is better to back-pressure data sources until that data is needed.

The advantage of a RxPY Observable is that it is generally faster
and more lightweight.


Implemented builders and operators (Flowable)
---------------------------------------------

### Create a Flowable

- `from_` - create a Flowable that emits each element of an iterable
- `from_iterable` - see `from_`
- `from_list` - create a Flowable that emits each element of a list
- `from_rx` - creates a Flowable from a rx Observable that buffers each element emitted by the Observable
- `return_value` - creates a Flowable that emits a single element
- `range` - creates a Flowable that emits elements defined by the range

### Transforming operators

- `filter` - emits only those element for which the given predicate holds
- `first` - emits the first element only
- `flat_map` - flattens a Flowable of Flowables
- `map` - applies a function to each element emitted by the Flowable
- `pairwise` - pairing two consecutive elements emitted by the Flowable
- `repeat_first` - repeat the first element by the Flowable forever (until disposed)
- `scan` - Applies an accumulator function over a Flowable sequence and returns each intermediate result.
- `zip_with_index` - The same as `map`, except that the selector function takes 
index in addition to the value

### Combining operators

- `concat` - consecutively subscribe each Flowable after the previous Flowable completes
- `controlled_zip` - combines the elements emitted by two Flowables 
into pairs in a controlled sequence. 
- `match` - combines the elements emitted by two Flowables into matching pairs.
- `zip` - combines the elements emitted by two Flowables into pairs in 
a strict sequence.

### Other operators

- `debug` - prints debug messages to the console
- `execute_on` - inject new scheduler that is used to subscribe Flowable
- `observe_on` - schedules element emitted by the Flowable on a dedicated scheduler

### Create a rx Observable

- `to_rx` - create a rx Observable from a Observable

Implemented builders and operators (MultiCast)
---------------------------------------------

### Create a MultiCast

- `to_multicast` - exposes a multicast flowable that allows multible subscriptions