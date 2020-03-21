"""
This example demonstrates the use-case of the join_flowables operator defined
on MultiCast objects. The join_flowables operator joins Flowables send
through the MultiCast object at different point in time.

This example merges two Flowables by first collecting two MultiCast
objects emitting each one Flowable.
"""

import rxbp

result = rxbp.multicast.join_flowables(
    rxbp.multicast.return_flowable(rxbp.range(4)),
    rxbp.multicast.return_flowable(rxbp.range(4)),
).pipe(
    rxbp.multicast.op.map(lambda v: v[0].merge(v[1]))
).to_flowable().run()

print(result)