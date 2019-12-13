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
    rxbp.multicast.op.share(and_zip),
    rxbp.multicast.op.share(merge_and_reduce),
).to_flowable().run()

print(result)
