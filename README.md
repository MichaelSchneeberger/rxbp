# ReactiveX Backpressure Library

**rxbp** is a Python library that integrates *backpressure* into the *Observable* via *Flowables*. 


## Features

- **Observable Pattern** - built on the reactive programming model.
- **Backpressure** - enables memory-safe handling of fast data producers and slow consumers.
- **RxPY compatibility** - interoperates with [RxPY](https://github.com/ReactiveX/RxPY/tree/master), bridging classic observables and backpressure-aware *Flowables*.
* **Continuation certificate**: ensures the execution of a Flowable completes, albeit with some computational overhead.


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

#The output will be [(0, 0, 0), (1, 1, 1), (2, 2, 2)]
print(result)
```

<!-- ## RxPY Compatibility -->

## Operations

### Create a Flowable

- `from_iterable` - create a *Flowable* that emits each element of an iterable
- `from_value` - create a *Flowable* that emits a single element
- `from_rx` - wrap a rx.Observable and exposes it as a *Flowable*, relaying signals in a backpressure-aware manner.
- `schedule_on` - schedule task on a dedicated scheduler and emit the scheduler

### Transforming operators

- `flat_map` - apply a function to each item emitted by the source and 
flattens the result
- `map` - map each element emitted by the source by applying the given 
function

### Combining operators

- `merge` - merge the elements of the *Flowable* sequences into a single *Flowable*
- `zip` - Create a new *Flowable* from two *Flowables* by combining their 
item in pairs in a strict sequence

### Other operators

- `share` - share a *Flowable* to possibly multiple subscribers

### Output functions

- `to_rx` - create a rx Observable from a Observable
