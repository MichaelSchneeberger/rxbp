
RxPy back-pressure extension
============================

An extension to the [RxPY](https://github.com/ReactiveX/RxPY) python 
library, that integrates back-pressure into observables.

The *rxbackpressure* library is inspired by [Monix](https://github.com/monix/monix), and has still experimental status. 

Differences from RxPY
---------------------




Implemented builders and operators
----------------------------------

### Create a Flowable

- `now` - creates a Flowable that emits a single element
- `from_` - create a Flowable that emits each element of an iterable
- `from_iterator` - see `from_`
- `from_rx` - creates a Flowable from a rx Observable that buffers each element emitted by the Observable

### Transforming operators

- `execute_on` - inject new scheduler that is used to subscribe Flowable
- `flat_map` - flattens a Flowable of Flowables
- `map` - applies a function to each element emitted by the Flowable
- `observe_on` - schedules element emitted by the Flowable on a dedicated scheduler
- `pairwise` - pairing two consecutive elements emitted by the Flowable
- `share` - exposes a multicast flowable that allows multible subscriptions
- `first` - emits the first element only
- `repeat_first` - repeat the first element by the Flowable forever (until disposed)
- `zip_with_index` - The same as `map`, except that the selector function takes 
index in addition to the value


### Combining operators

- `controlled_zip` - combines the elements emitted by two flowables 
into pairs in a controlled sequence. 
- `zip` - combines the elements emitted by two flowables into pairs in 
a strict sequence.

### Create a rx Observable

- `to_rx` - create a rx Observable from a Observable