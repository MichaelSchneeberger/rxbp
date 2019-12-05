import rxbp
import rxbp.depricated
from rxbp import op

rxbp.range(10).pipe(
    rxbp.depricated.share(lambda f1: f1.pipe(
        rxbp.op.match(f1.pipe(
            rxbp.op.filter(lambda v: v % 2 == 0)),
        )
    )),
).subscribe(print)