import reactivex as rx
import rxbp

rx_source = rx.of("Alpha", "Beta", "Gamma", "Delta", "Epsilon")

# convert Observable to Flowable
source = rxbp.from_rx(rx_source)

flowable = (
    source
    .map(lambda s: len(s))
    .filter(lambda i: i >= 5)
)

# convert Flowable to Observable
rxbp.to_rx(flowable).subscribe(lambda v: print(f"Received {v}"))