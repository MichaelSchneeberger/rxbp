Multicasting
------------
(02.10.2019)


An especially annoying problem with RxPY is that an *Observer* might miss 
elements, because it got subscribed too late to a hot *Observable*. A 
multi-casted *Observable* created by the `share` operator for example 
turns a *cold Observable* into a *hot Observable* the first time it gets 
subscribed. Thereafter, there is no guarantee that if another 
*Observable* subscribes to it some elements has not already been emitted.

When using the `share` operator, we normally don't really intend to make an 
*Observable* hot. We rather want to split the *Observable* into branches, 
apply operators on each branch and merge the branches with an operator like 
`zip` or `merge`. The resulting *Observable* is then supposed to be a cold
*Observable* again, which behaves the same way each time it is getting
subscribed.

The following code defines a hot multi-casted *Observable* by using the 
problematic `share` operator and subscribes to it twice. The second time it gets
subscribed, however, no elements are sent, because the *hot Observable*
is already completed.

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

Of course, if you know what you are doing you can implement an
*Observable* stream, where the subscription of *hot Observable*s happens
always before the first element is emitted. But the point is, that
there is no mechanism provided by RxPY that would prevent it and safe
us from these situations.

Let's see how we can do better. But instead of directly looking at the
end result, we will go step by step through the different ideas that lead
to the final implementation of the `MultiCast` type. The first step
was to change the `share` operator as follows:

``` python
def share(func: Callable[[MultiCastFlowable], Flowable])
```

The `share` operator does not directly create a *multi-cast Flowable*, 
but instead it returns a *unicast Flowable* and
exposes a *multi-cast Flowable* as an argument to the function `func`. Inside
the function `func`, the *multi-cast Flowable* can be used multiple
times as it is the case in *RxPY*. The following
example zips the elements from the same source but skips every second
element on the second *Flowable*.
 
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

This is nice, but what if we want to create a *multi-cast Flowable* in one
place and consume its elements in another place?
Having the `share` operator, this is possible by "tunneling" a Flowable. 
This converts a Flowable into a Flowable of
*multi-cast Flowables* of type `Flowable[Flowable]`. This is 
achieved by using `rxbp.return_value` inside the shared function as follows.
 
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

The `tunneled_shared` Flowable emits a single element of type `Flowable`,
which is then consumed by the `flat_map` operator downstream.

A "tunnel" can contain a single Flowable, but it can also contain multiple 
Flowables. For instance, we could tunnel a dictionary of Flowables of type
`Flowable[Dict[str, Flowable]`. A "tunnel" is therefore like a stream of
Flowables that can be consumed somewhere downstream.

In this example, a dictionary is send through the "tunnel" instead of a
single Flowable.

``` python
import rxbp
source1 = rxbp.range(10).pipe(
    rxbp.depricated.share(
        lambda input: rxbp.return_value({'input': input})),
    ),
)
```

`source1` represents the tunnel that emits a dictionary with a single
Flowable. We can either extend the dictionary by creating new Flowables 
derived from the single Flowable and append them to the dictionary. Or, we 
create a new Flowable from the single Flowable and merge it to the "tunnel" 
as shown in the following code.

``` python
source2 = source1.pipe(
    rxbp.op.share(lambda source: source.pipe(
        rxbp.op.flat_map(
            lambda fdict: fdict["input"].pipe(
                rxbp.op.zip(fdict["input"].pipe(
                    rxbp.op.filter(lambda v: v % 2 == 0)),
                ),
            )
        ),
        rxbp.op.share(
            lambda zipped: rxbp.return_flowable(
                {'zipped': zipped}
            )
        ),
        rxbp.op.merge(source),
    )),
)
```

In the end, we select a single Flowable and flat_map it, which will
convert the "tunneled" Flowable back to a normal Flowable.

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
* after each operation on that stream, we get a single Flowable back,
which represents the old stream and the operation we just added.
* in the end, we select a single Flowable from that stream of Flowables.

### MultiCast

"Tunneling" Flowables is tricky, because it requires the elements to be sent
throught a "tunnel" without delay. Even the back-pressure functionality 
of rxbp can introduce a delay. Futhermore, it requires the coordination
between sending Flowables through the "tunnel" and subscribing to them as
this needs to happen in a very specific order.

To make sure that all these requirements are fulfilled, a new object is
added to the rxbp library that implements the different operations on
a "tunnel". It is called `MultiCast`.

A `MultiCast` can be though of `rx.Observable[T2[Flowable[T1]]]`,
where `T1` is the element type of the Flowable, and `T2` boxes the 
Flowables into a user defined type like a dictionary.
The "tunnel" is internally implemented as an rx.Observable instead of 
a Flowable. The MultiCast provides operators that let you
safely transform sharable Flowables and, therefore, give a guarantee
that the subscription of a shared *Flowable* happens always before 
its first element is sent.

Like a *Flowable*, a *Multicast* exposes sources (`rxbp.multicast`) 
that create a *Multicast*, and operators (`rxbp.multicast.op`) that 
transform a *Multicast* into another *Multicast*. The *MultiCast* 
implements a `pipe` method that let you concatenate operations on 
the *MultiCast* objects.

A *Flowable* can 
be converter to a *Multicast* by the `rxbp.multicast.return_flowable` 
operator.