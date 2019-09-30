import rxbp

f = rxbp.to_multicast(rxbp.range(10)).pipe(
    rxbp.multicast.op.share(lambda base: base.pipe(
        rxbp.op.zip(base.pipe(
            rxbp.op.map(lambda v: v + 1),
            rxbp.op.filter(lambda v: v % 2 == 0)),
        ),
    )),
).to_flowable()
f.subscribe(print)