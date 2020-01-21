"""
This example uses the match operator to match a concatenated Flowable.
"""

import rxbp

f1 = rxbp.range(10).pipe(
    rxbp.op.concat(
        rxbp.range(8).pipe(
            rxbp.op.filter(lambda v: v%2),
        )
    ),
)

f2 = rxbp.range(10).pipe(
    rxbp.op.filter(lambda v: v%2),
    rxbp.op.concat(
        rxbp.range(8)
    ),
)

result = rxbp.match(f1, f2).run()

print(result)
