# ReactiveX Backpressure Library

**rxbp** is a Python library that integrates *backpressure* into the *Observable* via *Flowables*. 


## Features

- **Observable Pattern**: built on the reactive programming model.
- **Backpressure**: enables memory-safe handling of fast data producers and slow consumers.
- **Continuation certificate**: ensures that the execution of a Flowable completes, avoiding any continuation deadlock.
- **RxPY compatibility**: interoperates with *RxPY*, bridging classic observables and backpressure-aware *Flowables*.
- **Favor usability** - Favor an implementation that is simple, safe, and user-friendly, while accepting some computational overhead.


## Installation

You can install *rxbp* using pip:

```
pip install rxbp
```


## Example

``` python
import rxbp

source = rxbp.from_iterable(("Alpha", "Beta", "Gamma", "Delta", "Epsilon"))

flowable = (
    source
    .map(lambda s: len(s))
    .filter(lambda i: i >= 5)
    .tap(on_next=lambda v: print(f'Received {v}'))
)

# execute the flowable
rxbp.run(flowable)
```

<!-- ## Run a Flowable

`result = rxbp.run(flowable)`



## Share a Flowable -->


## Operations

### Create a Flowable

- `connectable` - create a *Flowable* whose source must be specified by the `connections` argument when calling the `run` function
- `count` - create a *Flowable* emitting 0, 1, 2, ...
- `create` - creates a *Flowable* from a *ContinuationMonad*
- `empty` - create a *Flowable* emitting no items
- `error` - create a *Flowable* emitting an exception
- `from_iterable` (or `from_`) - create a *Flowable* that emits each element of an iterable
- `from_value` (or `return_`) - create a *Flowable* that emits a single element
- `from_rx` - wrap a rx.Observable and exposes it as a *Flowable*, relaying signals in a backpressure-aware manner.
- `interval` - create a *Flowable* emitting an item after every time interval
- `repeat` - create a *Flowable* that repeats the given element
- `schedule_on` - schedule task on a dedicated scheduler
- `sleep` (or `delay`) - schedule task on a dedicated scheduler after a relative time
<!-- - `schedule_relative` - schedule task on a dedicated scheduler after a relative time
- `schedule_absolute` - schedule task on a dedicated scheduler after an absolute time -->

### Transforming operators

- `accumulate` - apply an accumulator function over a *Flowable* sequence and returns each intermediate result.
- `batch` - gathers items into batches of provided size
- `concat_map` - apply a function to each item emitted by the source and flatten the results sequentially
- `default_if_empty` - emits a given value if the source completes without emitting anything
- `filter` - emit only those items for which the given predicate holds
- `first` - emit the first element only
- `flat_map` - apply a function to each item emitted by the source and flattens the result
- `last` - emit last item
- `map` - map each element emitted by the source by applying the given function
- `reduce` - apply an accumulator function over a Flowable sequence and emits a single element
- `repeat` - returns a *Flowable* that will resubscribe to the source when the source completes
- `repeat_first` - return a *Flowable* that repeats the first element it receives from the source forever (until disposed).
- `skip` - skip the first n items
- `skip_while` - skip first items while the given predicate holds
- `take` - take the first n items
- `take_while` - take items while the given predicate holds
- `tap` - used to perform side-effects for notifications from the source *Flowable*
- `to_list` - create a new *Flowable* that collects the items from the source sequence, and emits a single item
- `zip_with_index` - zip each item emitted by the source with the enumerated index

### Combining operators

- `merge` - merge the items of the *Flowable* sequences into a single *Flowable*
- `zip` - Create a new *Flowable* from two *Flowables* by combining their 
item in pairs in a strict sequence

### Other operators

- `share` - share a *Flowable* to possibly multiple subscribers

### Output functions

- `to_rx` - create a rx Observable from a Observable


## *RxPY* integration

A *Flowable* can be created from an *RxPY* Observable using the `rxbp.from_rx` function.
Likewise, a *Flowable* can be converted to an *RxPY* Observable using the `rxbp.to_rx` function.
The example below demonstrates the two conversion:

``` python
import reactivex as rx
import rxbp

rx_source = rx.of("Alpha", "Beta", "Gamma", "Delta", "Epsilon")

# convert Observable to Flowable
source = rxbp.from_rx(rx_source)

flowable = (
    source
    .map(lambda s: len(s))
    .filter(lambda i: i >= 5)
)

# convert Flowable to Observable
rxbp.to_rx(flowable).subscribe(lambda v: print(f"Received {v}"))
```


## Reference

Below are some references related to this project:

* [continuationmonad](https://github.com/MichaelSchneeberger/continuationmonad/) is a Python library that implements stack-safe continuations based on schedulers.
<!-- * [donotation](https://github.com/MichaelSchneeberger/continuationmonad/) is a Python library that implements stack-safe continuations based on schedulers. -->
* [RxPY](https://github.com/ReactiveX/RxPY/tree/master) is rx extension for Python implementing the Observable pattern (without backpressure).
