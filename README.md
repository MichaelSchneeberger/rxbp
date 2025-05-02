# ReactiveX Backpressure Library

**rxbp** is a Python library that integrates *backpressure* into the *Observable* via *Flowables*. 


## Features

- **Observable Pattern**: built on the reactive programming model.
- **Backpressure**: enables memory-safe handling of fast data producers and slow consumers.
- **Continuation certificate**: ensures the execution of a Flowable completes, avoiding any continuation deadlock.
- **RxPY compatibility**: interoperates with [RxPY](https://github.com/ReactiveX/RxPY/tree/master), bridging classic observables and backpressure-aware *Flowables*.
- **Favor usability** - Favor an implementation that is simple, safe, and user-friendly, while accepting some computational overhead.

<!-- ## Installation -->

## Example

``` python
import rxbp

# Defines a flowable that emits up to 7 elements
range7 = rxbp.from_iterable(range(7))

# Defines a shared flowable that emits up to 5 elements
# The shared operator ensures that the downstream flowable is subscribed once
range5 = rxbp.from_iterable(range(3)).share()

# Zip elements of the three flowables
flowable = rxbp.zip((range5, range7, range5))

# Run flowable and collect received items in a list
result = flowable.run()

# The output will be [(0, 0, 0), (1, 1, 1), (2, 2, 2)]
print(result)
```

<!-- ## RxPY Compatibility -->

## Operations

### Create a Flowable

- `count` - create a *Flowable* emitting 0, 1, 2, ...
- `empty` - create a *Flowable* emitting no items
- `error` - create a *Flowable* emitting an exception
- `from_iterable` - create a *Flowable* that emits each element of an iterable
- `from_value` - create a *Flowable* that emits a single element
- `from_rx` - wrap a rx.Observable and exposes it as a *Flowable*, relaying signals in a backpressure-aware manner.
- `interval` - create a *Flowable* emitting an item after every time interval
- `schedule_on` - schedule task on a dedicated scheduler
- `schedule_relative` - schedule task on a dedicated scheduler after a relative time
- `schedule_absolute` - schedule task on a dedicated scheduler after an absolute time

### Transforming operators

- `accumulate` - apply an accumulator function over a *Flowable* sequence and returns each intermediate result.
- `default_if_empty` -
- `do_action` -
- `filter` - emit only those items for which the given predicate holds
- `first` - emit the first element only
- `flat_map` - apply a function to each item emitted by the source and flattens the result
- `last` - emit last item
- `map` - map each element emitted by the source by applying the given function
- `reduce` - apply an accumulator function over a Flowable sequence and emits a single element
- `repeat_first` - return a *Flowable* that repeats the first element it receives from the source forever (until disposed).
- `skip` - skip the first n items
- `skip_while` - skip the first items while the given predicate holds
- `take` - take the first n items
- `take_while` - take the first item while the given predicate holds
- `to_list` - create a new *Flowable* that collects the elements from the source sequence, and emits a single item
- `zip_with_index` - zip each item emitted by the source with the enumerated index

### Combining operators

- `merge` - merge the items of the *Flowable* sequences into a single *Flowable*
- `zip` - Create a new *Flowable* from two *Flowables* by combining their 
item in pairs in a strict sequence

### Other operators

- `share` - share a *Flowable* to possibly multiple subscribers

### Output functions

- `to_rx` - create a rx Observable from a Observable


## Reference

Below are some references related to this project:

* [continuationmonad](https://github.com/MichaelSchneeberger/continuationmonad/) is a Python library that implements stack-safe continuations based on schedulers.
