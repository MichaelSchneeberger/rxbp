import rxbp
import rxbp.depricated

from rxbp import op


def add_output1_to_dict():
    return op.flat_map(lambda fdict: fdict['input'].pipe(
        op.filter(lambda v: v%2 == 0),
        rxbp.depricated.share(lambda output1: rxbp.return_value({**fdict, 'output1': output1})),
    ))


def add_output2_to_dict():
    return op.flat_map(lambda fdict: fdict['input'].pipe(
        op.map(lambda v: v+100),
        rxbp.depricated.share(lambda output2: rxbp.return_value({**fdict, 'output2': output2})),
    ))


result = rxbp.range(10).pipe(
    rxbp.depricated.share(lambda input: rxbp.return_value({'input': input})),
    add_output1_to_dict(),
    add_output2_to_dict(),
    op.flat_map(lambda fdict: fdict['output1'].pipe(
        op.to_list(),
        op.zip(fdict['output2'].pipe(
            op.to_list(),
        ))
    ))
).run()

print(result)


# Multicast implementation
# ------------------------


result = rxbp.multicast.from_flowable(rxbp.range(10)).pipe(
    rxbp.multicast.op.map(lambda s: {'input': s}),
    rxbp.multicast.op.share(
        func=lambda fdict: fdict['input'].pipe(
            op.filter(lambda v: v % 2 == 0),
        ),
        selector=lambda fdict, o1: {**fdict, 'output1': o1},
    ),
    rxbp.multicast.op.share(
        func=lambda fdict: fdict['input'].pipe(
            op.map(lambda v: v + 100),
        ),
        selector=lambda fdict, o2: {**fdict, 'output2': o2},
    ),
    rxbp.multicast.op.share(
        func=lambda fdict: fdict['output1'].pipe(
            op.to_list(),
            op.zip(fdict['output2'].pipe(
                op.to_list(),
            ))
        ))
).to_flowable().run()

print(result)
