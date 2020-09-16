"""
This example demonstrates the use-case of the lift operator defined
on MultiCast objects. The lift operator lifts a `MultiCast[T1]` to
a `MultiCast[T2[MultiCast[T1]]]`, which enables to group `MultiCast`
objects within a MultiCast stream.
"""

import rxbp
from rxbp.multicast.multicast import MultiCast


def connect_and_zip(multicast: MultiCast):
    return rxbp.multicast.join_flowables(
        multicast,
        multicast,
    ).pipe(
        rxbp.multicast.op.map(lambda t: t[0].pipe(
            rxbp.op.zip(t[1]),
        )),
    )


def merge_and_reduce(multicast: MultiCast):
    return rxbp.multicast.merge(
        multicast,
        multicast,
    ).pipe(
        rxbp.multicast.op.merge_flowables(),
    )


result = rxbp.multicast.return_value(rxbp.range(5)).pipe(
    rxbp.multicast.op.lift(),
    rxbp.multicast.op.map(connect_and_zip),
    rxbp.multicast.op.map(merge_and_reduce),
    rxbp.multicast.op.flat_map(lambda m: m),
).to_flowable().run()

print(result)
