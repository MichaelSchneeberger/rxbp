
RxPy back-pressure extension
============================

![Build Status](https://github.com/MichaelSchneeberger/rxbackpressure/workflows/build/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/MichaelSchneeberger/rxbackpressure/badge.svg?branch=master)](https://coveralls.io/github/MichaelSchneeberger/rxbackpressure?branch=master)
![Package Publish Status](https://github.com/MichaelSchneeberger/rxbackpressure/workflows/pypi/badge.svg)

An extension to the [RxPY](https://github.com/ReactiveX/RxPY) python 
library, that integrates back-pressure into the *Observable* pattern
in form of *Flowables*.
 
The *rxbackpressure* library is inspired by [Monix](https://github.com/monix/monix), 
and **has still an experimental status**. 

Installation
------------

rxbp v3.x runs on Python 3.7 or above. To install rxbp alpha version:

```
pip3 install --pre rxbp
```

Example
-------

*rxbackpressure* has a similar syntax as RxPY.

```python
# example taken from RxPY
import rxbp

source = rxbp.from_(["Alpha", "Beta", "Gamma", "Delta", "Epsilon"])

composed = source.pipe(
    rxbp.op.map(lambda s: len(s)),
    rxbp.op.filter(lambda i: i >= 5)
)
composed.subscribe(lambda value: print(f"Received {value}"))
```

Integrate RxPY
--------------

A RxPY Observable can be converted to a *Flowable* by using the `from_rx` function.
Equivalently, a *Flowable* can be converted to a RxPY Observable 
by using the `to_rx` function.

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
composed.to_rx().subscribe(lambda value: print(f"Received {value}"))
```

Differences from RxPY
---------------------

### Flowable

Similar to an RxPY Observable, a *Flowable* implements a `subscribe` 
method, which is a mechanism that allows to describe a 
data flow from its source to some sink. The description is
done with *rxbackpressure* operators exposed by `rxbp.op`.

Like in functional programming, usings *rxbackpressure* operators 
does not create any mutable states, but rather concatenates functions 
without calling them yet. We first describe what we intend to 
do in form of a plan and then execute the plan. A *Flowable* is 
executed by calling its `subscribe` method. This will start a chain 
reaction, where each downsream *Flowables* calls the `subscribe` 
method of its upstream *Flowable* until
the sources start emitting the data. Once a *Flowable* is subscribed, we
allow it to have internal mutable states.
 
Compared to RxPY Observables, however, a *Flowable* uses `Observers` that are
able to back-pressure on an `on_next` method call. This has an effect
on how certain operators behave differently from the ones in RxPY.

### MultiCast (experimental)

A *MultiCast* is used when a *Flowable* emits elements to more than
one `Observer`, and can be though of a nested *Flowable* of type
 `rx.Observable[T[Flowable]]`.

This implementation is quite different from RxPY and there are good
reasons for that. In RxPY, there is an operator called `share`,
that turns an *Observable* into a so-called hot *Observable* allowing
multiple downstream subscribers to receive the same elements. Turning
an *Observable* hot produces a side-effect such that subsequent `subscribe`
calls don't propagate upstream. The following example calls the 
*Observable* twice, but the second time no elements are sent. 

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

In *rxbackpressure*, however, a *Flowable* can only be "shared" if it 
is nested inside a *MultiCast*. A *MultiCast* represents a collection 
of possibly hot *Flowables* and can be though of being of type
`rx.Observable[T[Flowable]]`, where `T` is defined by the user.
Operators on *MultiCasts* stream are exposed through `rxbp.multicast.op`.

```python
import rxbp

f = rxbp.multicast.from_flowable(rxbp.range(10)).pipe(
    rxbp.multicast.op.map(lambda base: base.pipe(
        rxbp.op.zip(base.pipe(
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

The `match` operator tries to match two *Flowables*, 
and raises an exception otherwise.
Two observables match if they have the same base or if there exists 
a mapping that maps 
one base to the base of the other *Flowable*. These mappings 
are called *selectors* and propagated internally when subscribing
to a *Flowable*.

If two *Flowables* have the same base, 
they should match in the sense of the `zip` operator,
e.g. every pair of elements that get zipped from the two
 *Flowables* should belong together.

```python
import rxbp

rxbp.range(10).pipe(
    rxbp.op.match(rxbp.range(10).pipe(
        rxbp.op.filter(lambda v: v % 2 == 0)),
    )
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

When to use a Flowable, when RxPY Observable?
-----------------------------------------

A *Flowable* is used when some asynchronous stage cannot process the
data fast enough, or needs to synchronize the data with some other event.
Let's take the `zip` operator for instance. It gets elements from
two or more sources and emits a tuple once it received one
element from each source. But what happens if one source emits the
elements before the others do? Without back-pressure, the `zip` operation
has to buffer the elements until it receives data from the other sources.
This might be ok depending on how much data needs to be buffered. But
sometimes we can not risk having too much data buffered somewhere in our
stream, which might lead to an out of memory exception. In this case,
back-pressure holds the data back until it is actually
needed.

The advantage of a RxPY Observable is that it is generally faster
and more lightweight.


Flowable
--------

### Create a Flowable

- `empty` - create a *Flowable* emitting no elements
- `from_` - create a *Flowable* that emits each element of an iterable
- `from_iterable` - see `from_`
- `from_list` - create a *Flowable* that emits each element of a list
- `from_range` - create a *Flowable* that emits elements defined by the range
- `from_rx` - wrap a rx.Observable and exposes it as a *Flowable*, relaying signals in a backpressure-aware manner.
- `return_value` - create a *Flowable* that emits a single element

### Transforming operators

- `filter` - emit only those elements for which the given predicate holds
- `first` - emit the first element only
- `flat_map` - apply a function to each item emitted by the source and 
flattens the result.
- `map` - map each element emitted by the source by applying the given 
function
- `map_to_iterator` - create a *Flowable* that maps each element emitted 
by the source to an iterator and emits each element of these iterators.
- `pairwise` - create a *Flowable* that emits a pair for each consecutive 
pairs of elements in the *Flowable* sequence
- `reduce` - Apply an accumulator function over a Flowable sequence and 
emits a single element
- `repeat_first` - Return a *Flowable* that repeats the first element it 
receives from the source forever (until disposed).
- `scan` - apply an accumulator function over a *Flowable* sequence and 
returns each intermediate result.
- `to_list` - Create a new *Flowable* that collects the elements from 
the source sequence, and emits a single element of type List.
- `zip_with_index` - zip each item emitted by the source with the 
enumerated index

### Combining operators

- `concat` - Concatentates *Flowable* sequences together by back-pressuring 
the tail *Flowables* until the current *Flowable* has completed
- `controlled_zip` - create a new *Flowable* from two *Flowables* by combining 
their elements in pairs. Which element gets paired with an element from 
the other *Flowable* is determined by two functions called `request_left` and 
`request_right`
- `match` - create a new *Flowable* from two *Flowables* by first filtering and 
duplicating (if necessary) the elements of each *Flowable* and zip the resulting 
*Flowable* sequences together
- `merge` - merge the elements of the *Flowable* sequences into a single *Flowable*
- `zip` - Create a new *Flowable* from two *Flowables* by combining their 
item in pairs in a strict sequence

### Other operators

- `buffer` - buffer the element emitted by the source without back-pressure until 
the buffer is full
- `debug` - print debug messages to the console
- `execute_on` - inject new scheduler that is used to subscribe the *Flowable*
- `observe_on` - schedule elements emitted by the source on a dedicated scheduler
- `set_base` - overwrite the base of the current Flowable sequence
- `share` - multicast the elements of the *Flowable* to possibly 
multiple subscribers

### Create a rx Observable

- `to_rx` - create a rx Observable from a Observable

MultiCast (experimental)
---------

### Create a MultiCast

- `empty` - create an empty *MultiCast*
- `from_flowable` - creates a *MultiCast* from a *Flowable* by making it
a *MultiCastFlowable*
- `from_iterable` - create a *MultiCast* from an iterable
- `from_observable` - create a *MultiCast* from an *rx.Observable*
- `return_value` - create a *MultiCast* that emits a single element

### Transforming operators

- `loop_flowable` - used to create a *Flowable* loop
- `filter` - only emits those *Multicast* values for which the given predicate hold.
- `flat_map` - maps each *Multicast* value by applying a given function and flattens the result.
- `lift` - lift the current `Observable[T1]` to a `Observable[T2[MultiCast[T1]]]`.
- `map` - maps each *Multicast* value by applying a given function.
- `merge` - merges two or more *Multicast* streams together.
- `reduce_flowable` - creates a *Multicast* that emits a single value
- `collect_flowables` - zips *Multicast*s emitting a single *Flowable* to a *Multicast* emitting a single value

### Other operators 

- `debug` - prints the objects flowing through the *Multicast* stream