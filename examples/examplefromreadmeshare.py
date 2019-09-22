import rxbp

rxbp.range(10).pipe(
    rxbp.op.share(lambda f1: f1.pipe(
        rxbp.op.zip(f1.pipe(
            rxbp.op.map(lambda v: v + 1),
            rxbp.op.filter(lambda v: v % 2 == 0)),
        )
    )),
).subscribe(print)