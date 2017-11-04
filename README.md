
RxPy backpressure extension
===========================

An extension to the [RxPY](https://github.com/ReactiveX/RxPY) python library, that takes the idea of backpressure a bit further.

Creating backpressured observables
----------------------------------

- to_backpressure - create the backpressured observable from a normal rx observable


Transforming backpressured observables
--------------------------------------

- flat_map
- map
- pairwise
- to_observable


Filtering backpressured observables
--------------------------------------

- first


Combining backpressured observables
--------------------------------------

- window - windows another backpressured observable via is_lower and is_higher functions and elements from this backpressured observable
- zip


Example
-------

Performing operations on hot observables with backpressure results in better memory handling.


```python
from rx import Observable

from rx_backpressure.subjects.buffered_subject import BufferedSubject

# create some buffered subjects
s1 = BufferedSubject()
s2 = BufferedSubject()
s3 = BufferedSubject()

# flat map operation
s2.flat_map(lambda v1, idx: s1.map(lambda v2, idx: v2*v1)) \
    .to_observable() \
    .filter(lambda v: 800 < v) \
    .subscribe(print, on_completed=lambda: print('completed'))

# zip operation
s1.zip(s3, lambda v1, v2: (v1, v2)) \
    .to_observable() \
    .filter(lambda v: 90 < v[0]) \
    .subscribe(print, on_completed=lambda: print('completed'))

# trigger hot observables
Observable.range(0, 100).subscribe(s1)
Observable.range(0, 10).subscribe(s2)
Observable.range(0, 100).subscribe(s3)
```