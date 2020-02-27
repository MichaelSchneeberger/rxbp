"""
This example demonstrates a use-case of the `controlled_zip` operator.

The goal of the use-case is to map time samples to a corresponding time interval.
The time samples and time intervals are represented by Flowables, which are then
combined via the `controlled_zip` operator. The resulting Flowable emits an element
for each time sample that fits some time interval.
"""

from random import random

import rxbp

# generate random time samples
time_samples = rxbp.range(100).scan(lambda acc, _: acc + random() / 5, initial=0)

# generate the time intervals of interest
time_intervals = rxbp.range(10).pairwise()

# map each time sample to the corresponding time interval
time_intervals.pipe(
    rxbp.op.controlled_zip(
        right=time_samples,
        request_left=lambda l, r: l[1] <= r,        # request a new time interval, if time sample is ahead
        request_right=lambda l, r: r < l[1],        # request a new time sample, if it lays within the current time
                                                    # interval or if it is behind.
        match_func=lambda l, r: l[0] <= r < l[1]    # only emit element, if time sample lays within the current time
                                                    # interval
    ),
).subscribe(print)
