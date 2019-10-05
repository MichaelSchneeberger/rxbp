An extension to the [RxPY](https://github.com/ReactiveX/RxPY) python 
library, that integrates back-pressure into observables.

Multicast
---------
(02.10.2019)

One annoying problem with RxPY is when an `Observer` misses an event,
because it got subscribed too late to a `Observable`. Especially,
when working with *multicasting*, it happens quite unexpected as
described [here](https://github.com/ReactiveX/RxPY/issues/309).
In fact, a *multicast* `Observable` turns a *cold* `Observable` into a 
*hot* `Observable` the first time it gets subscribed. Thereafter, 
there is no guarantee that another `Observable` can be subscribed to 
the *multicast*  `Observable` before the first elements get emitted. 

Of course, if you know what you are doing you can implement an
`Observable` stream, where the subscription of `Observable`s happens
always before the first elements are emitted. But the point is, that
there is no mechanism provided by RxPY that would prevent it and safe
us from these situations.

Another problem is that an RxPY `Observable` using *multicasting* 
doesn't behave the same way each time we subscribe it. You might
say: "Of course, this is because you have a *hot* `Observable`".
But the problem is, how should I know if an `Observable` is *hot* or 
not. Let's say, you get an `Observable` back from some third-party
function. Just from the object itself, there is no way to tell if it
is a *hot* or *cold* `Observable`.

