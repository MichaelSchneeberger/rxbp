import rxbp

source = rxbp.from_range(2).pipe(
    rxbp.op.map(lambda v: v+2),
)

result = rxbp.multicast.from_flowable(source).pipe(
    rxbp.multicast.op.map(lambda v: rxbp.from_range(v))
).to_flowable().run()

print(result)
