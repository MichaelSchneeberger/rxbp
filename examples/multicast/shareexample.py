"""
This example demonstrates the use-case of the share operator defined
on MultiCast objects.
"""

import rxbp
from rxbp.multicast.multicast import MultiCast


def connect_and_zip(multicast: MultiCast):
    return rxbp.multicast.join_flowables(
        multicast,
        multicast,
    ).pipe(
        rxbp.multicast.op.map(lambda t: t[0].zip(t[1]))
    )


def merge_and_reduce(multicast: MultiCast):
    return rxbp.multicast.merge(
        multicast,
        multicast,
    ).pipe(
        rxbp.multicast.op.collect_flowables(),
    )


result = rxbp.multicast.return_flowable(rxbp.range(10)).pipe(
    rxbp.multicast.op.share(connect_and_zip),
    rxbp.multicast.op.share(merge_and_reduce),
).to_flowable().run()

print(result)
