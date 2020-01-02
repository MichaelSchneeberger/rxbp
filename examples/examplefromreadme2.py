"""
This example is used in the README.md

It converts a RxPY Observable to a Flowable by using the `rxbp.from_rx`
operator, and back to a RxPY Observable by using the `rxbp.to_rx`.
"""

import rx
import rxbp
from rxbp import op

rx_source = rx.of("Alpha", "Beta", "Gamma", "Delta", "Epsilon")

# convert Observable to Flowable
source = rxbp.from_rx(rx_source)

composed = source.pipe(
    op.map(lambda s: len(s)),
    op.filter(lambda i: i >= 5),
)

# convert Flowable to Observable
composed.to_rx().subscribe(lambda value: print(f"Received {value}"))