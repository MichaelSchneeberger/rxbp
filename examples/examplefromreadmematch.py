import rxbp


rxbp.range(10).pipe(
    rxbp.op.match(rxbp.range(10).pipe(
        rxbp.op.filter(lambda v: v % 2 == 0)),
    )
).subscribe(print)
