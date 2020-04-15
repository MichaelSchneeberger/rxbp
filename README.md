
RxPy back-pressure extension
============================

![Build Status](https://github.com/MichaelSchneeberger/rxbackpressure/workflows/build/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/MichaelSchneeberger/rxbackpressure/badge.svg?branch=master)](https://coveralls.io/github/MichaelSchneeberger/rxbackpressure?branch=master)
![Package Publish Status](https://github.com/MichaelSchneeberger/rxbackpressure/workflows/pypi/badge.svg)

*rxbp* is an extension to the [RxPY](https://github.com/ReactiveX/RxPY) python 
library, that integrates back-pressure into the *Observable* pattern
in form of *Flowables*.
 
The *rxbp* library is inspired by [Monix](https://github.com/monix/monix), 
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

A RxPY Observable can be converted to a *Flowable* by using the `rxbp.from_rx` function.
Equivalently, a *Flowable* can be converted to an RxPY Observable 
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

Similar to an RxPY Observable, a *Flowable* implements a `subscribe` method,
which is a mechanism that allows to describe a data flow from its source to 
a sink. The description is done with *rxbp* operators exposed by `rxbp.op`.

Like in functional programming, usings *rxbp* operators 
does not create any mutable states, but rather concatenates functions 
without calling them yet. We first describe what we intend to 
do in form of a plan and then execute the plan. A *Flowable* is 
executed by calling its `subscribe` method. This will start a chain 
reaction, where each downsream *Flowables* calls the `subscribe` 
method of its upstream *Flowable* until
the sources start emitting the data. Once a *Flowable* is subscribed, we
allow it to have internal mutable states.
 
Compared to RxPY Observables, however, a *Flowable* uses `Observers` that are
able to back-pressure on an `on_next` method call. This has the effect that
certain operators behave differently from the ones in RxPY.

### MultiCast (experimental)

A *MultiCast* is used when a *Flowable* emits elements to more than one `Observer`, 
and can be though of a nested *Flowable* of type `Observable[T[Flowable]]`.

The syntax to *multi-cast* a Flowable is quite different from RxPY and there are good
reasons for that. In RxPY, there is an operator called `share`, that turns an *Observable* 
into a so-called hot *Observable* allowing multiple downstream subscribers to receive the 
same elements. The first `subscribe` call has the side-effect that subsequent `subscribe` 
calls will not propagate upstream, but register themselves to the hot *Observable*.
The following example illustrates the side-effect that happens when a shared *Observable*
is subscribed for the first time.

``` python
import rx
from rx import operators as op

o = rx.range(4).pipe(
    op.share(),
)

o.subscribe(print)
o.subscribe(print)      # the second time no elements are sent
```

The previous code outputs:

```
0
1
2
3
```

In *rxbp*, however, the elements of a *Flowable* sequence can only be multi-casted,
if the *Flowable* is nested inside a *MultiCast*. This can be done with the 
`rxbp.multicast.return_flowable` function. `return_flowable` takes a *Flowable*, a
list of *Flowables* or a dictionary of *Flowables* and creates a *MultiCast* that
emits the nested *Flowables*. Similarly to a *Flowable*, a *MultiCast* implements a `pipe`
method that takes a sequence of *MultiCast* operators, which are exposed by 
`rxbp.multicast.op`.

```python
import rxbp

f = rxbp.multicast.return_flowable(rxbp.range(10)).pipe(
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

The `match` operator tries to match two *Flowables*, and raises an exception otherwise.
Two *Flowables* match if they have the same base or if there exists a mapping that maps 
one base to the base of the other *Flowable*. These mappings propagated internally when 
subscribing to a *Flowable*.

If two *Flowables* match, the elements of each *Flowable* sequence are filtered and
dublicated (if necessary) first and then zipped together. The following example creates
two *Flowables* where one is having base *10* and the other contains a mapping from
base *10* to it's own base *None* (base *None* refers to a unknown *Flowable* sequence). 
The `match` operator applies the mapping to the Flowable of base *10* such that every
second element is selected due to `v % 2`.


```python
import rxbp

rxbp.from_range(10).pipe(
    rxbp.op.match(rxbp.from_range(10).pipe(
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

A *Flowable* is used when some asynchronous stage cannot process the data fast enough, 
or needs to synchronize the data with some other event. Let's take the `zip` operator 
as an example. It receives elements from two or more sources and emits a tuple once it 
received one element from each source. But what happens if one source emits the 
elements before the other does? Without back-pressure, the `zip` operator has to buffer 
the elements from the eager source until it receives the elements from the other source.
This might be ok depending on how many elements need to be buffered. But often it is too
risky to buffer elements somewhere in our stream as it potentially leads to an 
out of memory exception. The back-pressure capability prevents buffers to grow by holding 
the data back until it is actually needed.

The advantage of a RxPY Observable is that it is generally faster and more lightweight.


Flowable
--------

### Create a Flowable

- `empty` - create a *Flowable* emitting no elements
- `from_` - create a *Flowable* that emits each element of an iterable
- `from_iterable` - see `from_`
- `from_list` - create a *Flowable* that emits each element of a list
- `from_range` - create a *Flowable* that emits elements defined by the range
- `from_rx` - wrap a rx.Observable and exposes it as a *Flowable*, relaying signals in a backpressure-aware manner.
- `return_flowable` - create a *Flowable* that emits a single element

### Transforming operators

- `filter` - emit only those elements for which the given predicate holds
- `first` - emit the first element only
- `flat_map` - apply a function to each item emitted by the source and 
flattens the result
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
- `share` - multi-cast the elements of the *Flowable* to possibly 
multiple subscribers

### Create a rx Observable

- `to_rx` - create a rx Observable from a Observable

MultiCast (experimental)
------------------------

### Create a MultiCast

- `empty` - create a *MultiCast* emitting no elements
- `return_flowable` - turn zero or more *Flowables* into multi-cast *Flowables* 
emitted as a single element inside a  *MultiCast*
- `return_` - create a *MultiCast* emitting a single element
- `from_iterable` - create a *MultiCast* from an iterable
- `from_rx_observable` - create a *MultiCast* from an *rx.Observable*
- `from_flowable` - (similar to `from_rx_observable`) create a *MultiCast* 
that emits each element received by the Flowable

### Transforming operators

- `default_if_empty` - either emits the elements of the source or a default element
- `filter` - emit only those *MultiCast* for which the given predicate hold
- `flat_map` - apply a function to each item emitted by the source and 
flattens the result
- `lift` - lift the current `MultiCast[T1]` to a `MultiCast[T2[MultiCast[T1]]]`.
- `map` - map each element emitted by the source by applying the given 
function
- `merge` - merge the elements of the *MultiCast* sequences into a single *MultiCast*

### Transforming operators (Flowables)

- `join_flowables` - zip one or more *Multicasts* (each emitting a single *Flowable*)
 to a *Multicast* emitting a single element (tuple of *Flowables*)
- `loop_flowables` - create a loop inside *Flowables*
- `collect_flowables` - create a *Multicast* that emits a single element containing 
the reduced *Flowables* of the first element sent by the source

### Other operators 

- `debug` - print debug messages to the console
- `observe_on` - schedule elements emitted by the source on a dedicated scheduler
- `share` - multi-cast the elements of the source to possibly 
multiple subscribers