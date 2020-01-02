"""
This example is used in the README.md

It is taken from RxPY documentation and translated to rxbackpressure
syntax.
"""

import rxbp
from rxbp import op

source = rxbp.from_(["Alpha", "Beta", "Gamma", "Delta", "Epsilon"])

composed = source.pipe(
    op.map(lambda s: len(s)),
    op.filter(lambda i: i >= 5)
)
composed.subscribe(lambda value: print(f"Received {value}"))