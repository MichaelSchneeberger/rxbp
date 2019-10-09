Multicasting
------------
(02.10.2019)



One annoying problem with RxPY is when an *Observer* misses an event,
because it got subscribed too late to an *Observable*. Especially,
when working with *multicasting*, it happens quite unexpected as
described [here](https://github.com/ReactiveX/RxPY/issues/309).
In fact, a *multicast Observable* (created by `publish` or `share` 
operator) turns a *cold Observable* into a 
*hot Observable* the first time it gets subscribed. Thereafter, 
there is no guarantee that another *Observable* can be subscribed
before the first elements get emitted. 

Of course, if you know what you are doing you can implement an
*Observable* stream, where the subscription of *Observable*s happens
always before the first elements are emitted. But the point is, that
there is no mechanism provided by RxPY that would prevent it and safe
us from these situations.

Let's see how we can do better. But instead of directly looking at the
end result, we will step by step go through the different ideas that lead
to the final implementation of the `MultiCast` type. The first step
was to omit the `publish` operator in rxbackpressure, and change 
the `share` operator as follows:

``` python
def share(func: Callable[[MultiCastFlowable], Flowable])
```

The `share` operator takes a function `func` as argument. But instead
of creating a multicast Flowable, it creates a unicast Flowable and
exposes a multicast Flowable as argument to the function `func`. Inside
the function `func`, the multicast Flowable can used multible
times as it the case for the shared Observable in RxPY. The following
example zips the elements from the same source but skips every second
element on the second zip input.
 
``` python
``` python
import rxbp.depricated
import rxbp

rxbp.range(10).pipe(
    rxbp.depricated.share(lambda f1: f1.pipe(
        rxbp.op.zip(f1.pipe(
            rxbp.op.filter(lambda v: v % 2 == 0)),
        )
    )),
).subscribe(print)
```
The previous code outputs:

```
(0, 0)
(1, 2)
(2, 4)
(3, 6)
(4, 8)
```

### Tunneling a shared Flowable

This is nice, but often we want to create a multicast Flowable in one
place and consume its elements in another place. With the `share` 
operator, this was possible by "tunneling" (or lifting) a Flowable. 
By "tunneling", we mean to convert a Flowable to a Flowable of
multicast Flowable(s) with type `Flowable[Flowable]`. This is 
achieved by using `rxbp.return_value` inside the shared function.
The multicast Flowable can then be consumed by using `rxbp.op.flat_map`
on the tunneled Flowable.
 
``` python
``` python
import rxbp.depricated
import rxbp

# create a tunneled shared Flowable in one place
tunneled_shared = rxbp.range(10).pipe(
    rxbp.depricated.share(lambda f1: rxbp.return_value(f1)),
)

# consume the shared Flowable in another place
tunneled_shared.pipe(
    rxbp.op.flat_map(lambda f1: f1.pipe(
        rxbp.op.zip(f1.pipe(
            rxbp.op.filter(lambda v: v % 2 == 0)),
        ),
    )),
).subscribe(print)
```

The next step is to "tunnel" (or lift) a shared Flowable and use it
in different places and not just the one where the elements get zipped
with itself. To accomblish this, we create a Flowable of shared 
Flowables, where new Flowable definitions are added to that stream.
If we like to consume one of these shared Flowables, we would filter
out the one we want. To do this, however, we better box it into some
type. In the following example, we pack it into a type called `Source`.

``` python
``` python
import rxbp.depricated
from dataclasses import dataclass

import rxbp
from rxbp.flowable import Flowable


@dataclass
class Source:
    flowable: Flowable

source1 = rxbp.range(10).pipe(
    rxbp.depricated.share(lambda f1: rxbp.return_value(Source(flowable=f1))),
)
```

Now we can consume the flowable boxed into `Source` by applying the
appropriate filter, perform some operation, and merge the result
back to the stream.

``` python
@dataclass
class Zipped:
    flowable: Flowable

source2 = source1.pipe(
    rxbp.op.share(lambda source: source.pipe(
        rxbp.op.filter(lambda v: isinstance(v, Source)),
        rxbp.op.flat_map(
            lambda v: v.flowable.pipe(
                rxbp.op.zip(v.flowable.pipe(
                    rxbp.op.filter(lambda v: v % 2 == 0)),
                ),
            )
        ),
        rxbp.op.share(
            lambda f2: rxbp.return_value(Zipped(flowable=f2))
        ),
        rxbp.op.merge(source),
    )),
)
```

In the end, we select a shared Flowable and flat_map it.

``` python
# source2.pipe(
#     rxbp.op.filter(lambda v: isinstance(v, Zipped)),
#     rxbp.op.flat_map(lambda v: v.flowable)
# ).subscribe(print)

source2.pipe(
    rxbp.op.filter(lambda v: isinstance(v, Source)),
    rxbp.op.flat_map(lambda v: v.flowable)
).subscribe(print)
```

To summarize:

* instead of sharing a Flowable directly as it is 
implemented in RxPY, we lift a Flowable into a stream of type
 `Flowable[Flowable]`.
* after each operation on that stream, we get a single Flowable back
that represents the old stream and the operation we just added. Drawing
this as a graph, we would get a linear sequence of operations.
* in the end, we select a single Flowable from that stream of Flowables.

### MultiCast

The `MultiCast` can be though of as a `Flowable[T2[Flowable[T1]]]`,
where `T1` is the element type of the (possibly) shared
Flowables. `T2` boxes the shared Flowables into a user defined type.
And, the outer Flowable is how this stream is internally represented
by the MultiCast. The MultiCast provides operators that let you
safely transform shared Flowables and, therefore, give a guarantee
that the subscription of a shared *Flowable* happens always before 
its first element get emitted.

Like a *Flowable*, a *Multicast* exposes sources (`rxbp.multicast`) 
that create a *Multicast*, and operators (`rxbp.multicast.op`) that 
transform a *Multicast*. If multicasting is required,
a *Flowable* can be converter to a *Multicast* by the 
`rxbp.multicast.from_flowable` function. The *MultiCast* implements
a pipe method to concatenate operations on the *MultiCast*. 