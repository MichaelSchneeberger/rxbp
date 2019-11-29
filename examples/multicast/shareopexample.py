"""
This example demonstrates a use-case of a MultiCast.
"""

import rxbp
from rxbp.multicast.flowabledict import FlowableDict

base = FlowableDict({'input': rxbp.range(10)})

result = rxbp.multicast.from_flowable(base).pipe(               # start the multicast from a Flowable
    rxbp.multicast.op.share(                                    # create a new shared Flowable
        func=lambda fdict: fdict['input'].pipe(                 # ... by creating it from the dictionary
            rxbp.op.filter(lambda v: v % 2 == 0),
        ),
        selector=lambda fdict, o1: fdict + {'output1': o1},    # ... and adding the shared Flowable to the dictionary
    ),
    rxbp.multicast.op.share(                                    # create a new shared Flowable
        func=lambda fdict: fdict['input'].pipe(
            rxbp.op.map(lambda v: v + 100),
        ),
        selector=lambda fdict, o2: fdict + {'output2': o2},
    ),
    rxbp.multicast.op.share(                                    # create a new shared Flowable
        func=lambda fdict: fdict['output1'].pipe(
            rxbp.op.to_list(),
            rxbp.op.zip(fdict['output2'].pipe(
                rxbp.op.to_list(),
            ))
        ))                                                      # return a single Flowable
).to_flowable().run()                                           # convert multi-cast to a (single) flowable

print(result)
