"""
This example demonstrates a use-case of merging two MultiCast objects.

Merging two MultiCast objects is almost equivalent to do the following:

source1: rx.Observable[Flowable]
source2: rx.Observable[Flowable]
source1.pipe(
    op.merge(source2)
)

"""

import rxbp

m1 = rxbp.multicast.return_flowable(rxbp.range(10))
m2 = rxbp.multicast.return_flowable(rxbp.range(5))


result = rxbp.multicast.merge(m1, m2).pipe(
    rxbp.multicast.op.collect_flowables(),
).to_flowable().run()

print(result)