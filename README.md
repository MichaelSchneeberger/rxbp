
RxPy back-pressure extension
============================

An extension to the [RxPY](https://github.com/ReactiveX/RxPY) python 
library, that integrates back-pressure into observables.

The *rxbackpressure* library is inspired by [Monix](https://github.com/monix/monix), and has still experimental status. 

Implemented builders and operators
----------------------------------

### Create a Flowable

- `now` - creates a Flowable that emits a single element
- `from_` - create a Flowable that emits each element of an iterable
- `from_iterator` - see `from_`
- `from_rx` - creates a Flowable from a rx Observable that buffers each element emitted by the Observable

### Transforming operators

- `execute_on`
- `flat_map` - flatten inner Observable emissioned by the outer SubFlowObservable into a single Observable
- `map` - transform the items emitted by an Observable by applying a function to each item
- `map_count` - The same as `map`, except that the selector function takes index in addition to the value
- `observe_on`
- `pairwise` - pairing two consecutive items emitted by an Observable
- `replay`
- `to_list` - Returns an observable sequence emitting a single element of type list containing all the elements of the source sequence
- `share` - Converts this observable into a multicast observable that backpressures only after each subscribed
observer backpressures.
- `first` - emit only the first item, or the first item that meets a condition from an Observable
- `repeat_first` - repeat the first item forever


### Combining operators

- `window` - forward each item from the left Observable by attaching an inner Observable to it. Subdivide or reject
items from the right Observable via is_lower and is_higher functions, and emit each item of a subdivision (or window)
in the inner Observable
- `controlled_zip`
- `zip` - combine the emissions of multiple Observables together via a specified function and emit single items for 
each combination based on the results of this function

### Create a rx Observable

- `to_rx` - create a rx Observable from a Observable