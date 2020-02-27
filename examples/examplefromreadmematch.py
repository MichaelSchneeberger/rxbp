"""
This example is used in the README.md

It "matches" two Flowables, where one has a "numerical base"
of 10 (because it got created by a `range` of 10) and the other
has a mapping from that same base (because it got created by a
`range` of 10 and `filter` by modulo 2). The mapping look like:

0 -> take
1 -> don't take
2 -> take
3 -> don't take
4 -> ...
"""

import rxbp


rxbp.range(10).pipe(
    rxbp.op.match(rxbp.range(10).pipe(
        rxbp.op.filter(lambda v: v % 2 == 0)),
    )
).subscribe(print)
