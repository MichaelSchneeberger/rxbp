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
composed.to_rx().subscribe(lambda value: print("Received {0}".format(value)))