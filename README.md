
RxPy backpressure extension
===========================

```python
from rx import Observable

from rxpy_backpressure.subjects.buffered_subject import BufferedSubject

s1 = BufferedSubject()
s2 = BufferedSubject()
s3 = BufferedSubject()

s2.flat_map(lambda v1, idx: s1.map(lambda v2, idx: v2*v1)) \
    .to_observable() \
    .filter(lambda v: 800<v) \
    .subscribe(print, on_completed=lambda: print('completed'))

s1.zip(s3, lambda v1, v2: (v1, v2)) \
    .to_observable() \
    .filter(lambda v: 90 < v[0]) \
    .subscribe(print, on_completed=lambda: print('completed'))


Observable.range(0, 100).subscribe(s1)
Observable.range(0, 10).subscribe(s2)
Observable.range(0, 100).subscribe(s3)
```