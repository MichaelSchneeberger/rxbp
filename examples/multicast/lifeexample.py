import rxbp
from rxbp.multicast.multicast import MultiCast


def and_zip(multicast: MultiCast):
    return rxbp.multicast.zip(
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
        rxbp.multicast.op.reduce(),
    )


result = rxbp.multicast.from_flowable(rxbp.range(10)).pipe(
    rxbp.multicast.op.lift(lambda m: m),
    rxbp.multicast.op.map(and_zip),
    rxbp.multicast.op.map(merge_and_reduce),
    rxbp.multicast.op.flat_map(lambda m: m),
).to_flowable().run()

print(result)