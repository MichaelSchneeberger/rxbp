"""
This example uses the match operator to match a concatenated Flowable.
"""

import rxbp

f1 = rxbp.indexed.range(10).pipe(
    rxbp.op.filter(lambda v: v%2),
    rxbp.op.concat(
        rxbp.indexed.range(8),
    ),
)

f2 = rxbp.indexed.range(10).pipe(
    rxbp.op.concat(
        rxbp.indexed.range(8).pipe(
            rxbp.op.filter(lambda v: v%2),
        )
    ),
)

m1 = rxbp.indexed.match(f1, f2).pipe(
    rxbp.op.filter(lambda t: t[0]%5),
)


f3 = rxbp.indexed.range(10).pipe(
    rxbp.op.concat(
        rxbp.indexed.range(8)
    ),
)


m2 = rxbp.indexed.match(m1, f3)

result = m2.run()
print(result)
