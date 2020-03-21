"""
This example demonstrates a use-case of the collect_flowables operator.
The collect_flowables operator reduces series of Flowables emitted by
the multi-cast object by merging the elements emitted by each
Flowable. The multicast created by the collect_flowables operator emits
a single element.

Besides a single Flowable, the collect_flowables operator
can also be applied to a dictionary of Flowables or an object
of type FlowableStateMixin.
"""
import rxbp

# collect_flowables the following two dictionaries, such that the new multicast
# emits a single element: {'val1': flowable1, 'val2': flowable2} where
# flowable1 emits all elements associated to 'val1' and flowable2 emits
# all elements associated to 'val2'
base1 = {'val1': rxbp.range(5), 'val2': rxbp.range(5).map(lambda v: v+100)}
base2 = {'val1': rxbp.range(3).map(lambda v: v+10), 'val2': rxbp.range(3).map(lambda v: v+110)}

result = rxbp.multicast.return_flowable(base1).pipe(
    rxbp.multicast.op.merge(
        rxbp.multicast.return_flowable(base2)
    ),
    rxbp.multicast.op.collect_flowables(),
    rxbp.multicast.op.map(lambda v: v['val1'].zip(v['val2'])),
).to_flowable().run()

print(result)

# collect_flowables single Flowable
# ----------------------

# the sample example, but with just one Flowable
result = rxbp.multicast.return_flowable(rxbp.range(5)).pipe(
    rxbp.multicast.op.merge(
        rxbp.multicast.return_flowable(rxbp.range(3))
    ),
    rxbp.multicast.op.collect_flowables(),
).to_flowable().run()

print(result)