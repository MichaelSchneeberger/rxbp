"""
This example demonstrates the use-case of the connect_flowable operator defined
on MultiCast objects. The connect_flowable operator pairs Flowables send
through the MultiCast object at different point in time.

This example merges two Flowables by first zipping two MultiCast
objects emitting each one Flowable.
"""

import rxbp

result = rxbp.multicast.connect_flowable(
    rxbp.multicast.from_flowable(rxbp.range(4)),
    rxbp.multicast.from_flowable(rxbp.range(4)),
).pipe(
    rxbp.multicast.op.map(lambda v: v[0].merge(v[1]))
).to_flowable().run()

print(result)