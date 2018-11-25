
RxPy back-pressure extension
============================

An extension to the [RxPY](https://github.com/ReactiveX/RxPY) python 
library, that integrates back-pressure into observables.

*Rxbackpressure* has an experimental status. 

The current motivation of this library is to use it for signal processing applications.


Implemented operators
---------------------

### Creating backpressured observables

- `now`
- `from_` - create a new Observable that emits each element of an iterable
- `from_iterator`
- `to_rxbackpressure` - create an Observable from a rx Observable


### Transforming backpressured observables

- `cache`
- `execute_on`
- `flat_map` - flatten inner Observable emissioned by the outer SubFlowObservable into a single Observable
- `flat_zip`
- `map` - transform the items emitted by an Observable by applying a function to each item
- `map_count` - The same as `map`, except that the selector function takes index in addition to the value
- `observe_on`
- `pairwise` - pairing two consecutive items emitted by an Observable
- `replay`
- `to_list` - Returns an observable sequence emitting a single element of type list containing all the elements of the source sequence
- `share` - Converts this observable into a multicast observable that backpressures only after each subscribed
observer backpressures.


### Filtering back-pressured observables

- `first` - emit only the first item, or the first item that meets a condition from an Observable
- `repeat_first` - repeat the first item forever


### Combining back-pressured observables

- `window` - forward each item from the left Observable by attaching an inner Observable to it. Subdivide or reject
items from the right Observable via is_lower and is_higher functions, and emit each item of a subdivision (or window)
in the inner Observable
- `controlled_zip`
- `zip` - combine the emissions of multiple Observables together via a specified function and emit single items for 
each combination based on the results of this function

### Creates an rx observables

- `to_rx` - create a rx Observable from a Observable