import rxbp


rxbp.multicast.from_flowable(key='input', source=rxbp.range(10).pipe(
)).pipe(
    rxbp.multicast.op.defer(
        func=lambda mc: mc.pipe(
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
            ]),
        ),
        initial=[1, 2],
    ),
).to_flowable().pipe(
).subscribe(print)