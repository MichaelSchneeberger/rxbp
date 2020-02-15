"""
This example demonstrates a use-case of the loop_flowables operator. The loop_flowables
operator is used to create a loop within Flowables, e.g. it defines
Flowables that emit elements derived from element previously send by
these Flowables. An initial value is required to start the loop, e.g.
start sending elements. A loop is only stopped by back-pressuring the
flow to a stop.

The loop_flowables operator takes two arguments `initial` and `func`, which are
used to define the initial elements send out of the loop and a function
that maps the Flowables send to the loop_flowables operator and the deferred
Flowables to some output. The output has to match the initial values.

Example of a matching `initial` and `func` types:

    initial: [int, int]
    func: {0: Flowable[int], 1: Flowable[int], 'input': Flowable[int]}
          => {0: Flowable[int], 1: Flowable[int]}

"""
import rxbp


result = rxbp.multicast.return_flowable({'input': rxbp.range(10)}).pipe(
    rxbp.multicast.op.loop_flowables(                                        # create a loop with two Flowables
        initial=[1, 2],                                             # define the first elements sent by these Flwables
        func=lambda mc: mc.pipe(                                    # define the stream between where the loop starts ...
            rxbp.multicast.op.map(lambda t: [
                t['input'].pipe(
                    rxbp.op.zip(t[0], t[1]),
                    rxbp.op.map(lambda v: sum(v)),
                ),
                t['input'].pipe(
                    rxbp.op.zip(t[1].pipe(
                    )),
                    rxbp.op.map(lambda v: sum(v)),
                ),
            ]),                                                     # ... to where the loops ends
        ),
    ),
    rxbp.multicast.op.map(lambda t: t[0]),
).to_flowable().run()

print(result)

# the same example as before, but this time a dictionary is returned
# instead of a list.
result = rxbp.multicast.return_flowable({'input': rxbp.range(10)}).pipe(
    rxbp.multicast.op.loop_flowables(
        func=lambda mc: mc.pipe(
            rxbp.multicast.op.map(lambda t: {
                **t,
                'a': t['input'].pipe(
                    rxbp.op.zip(t['a'], t['b']),
                    rxbp.op.map(lambda v: sum(v)),
                ),
                'b': t['input'].pipe(
                    rxbp.op.zip(t['b'].pipe(
                    )),
                    rxbp.op.map(lambda v: sum(v)),
                ),
            }),
        ),
        initial={'a': 1, 'b': 2},
    ),
    # rxbp.multicast.op.map(lambda t: t['a']),
).to_flowable().run()

print(result)