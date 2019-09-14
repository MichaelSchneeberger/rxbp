"""
This example demonstrates how elements can be send in batches over the Flowable
stream.

All rxbp sources (should) have an argument `batch_size`, which defines the
number of elements that flow in a batch through the Flowable stream. Increasing
the `batch_size` can result in much lower execution time as demonstrated in this
example. In this case, it beats the RxPY implementation by a factor of at least 2.
"""

import time
from random import random

import rxbp
import rx

from rxbp import op
from rx import operators as rxop


# number of samples that are sent through the test stream
n_samples = int(1e6)

# batch size used only in rxbp
batch_size = 100

# rxbp test stream
start = time.time()
rxbp.range(n_samples, batch_size=batch_size).pipe(
    op.map(lambda _: 2 * random() - 1),
    op.scan(lambda acc, v: acc + v, initial=0),
).subscribe()
print('to scan over {} it takes rxbp = {}s'.format(n_samples, time.time()-start))

# rx test stream
start = time.time()
rx.from_(range(n_samples)).pipe(
    rxop.map(lambda _: 2 * random() - 1),
    rxop.scan(lambda acc, v: acc + v, 0),
).subscribe()
print('to scan over {} it takes rx = {}s'.format(n_samples, time.time()-start))
