"""
This example is used in the README.md

It creates a MultiCast object from a Flowable by using the `return_flowable`
function. The Flowable boxed into a MultiCast object can now be
subscribed to more than one observer. In this example, we use it to join_flowables
it with itself. The result is that a new Flowable is created that emits
paired elements from the same source.

Note: that the new Flowable does not have the "sharing" behavior, see
`rxbp.op.share` operator for more information.
"""

import rxbp

f = rxbp.multicast.return_flowable(rxbp.range(10)).pipe(
    rxbp.multicast.op.map(lambda base: base.pipe(
        rxbp.op.zip(base.pipe(
            rxbp.op.map(lambda v: v + 1),
            rxbp.op.filter(lambda v: v % 2 == 0)),
        ),
    )),
).to_flowable()
f.subscribe(print)