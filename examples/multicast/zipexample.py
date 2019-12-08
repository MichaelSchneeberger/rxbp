import rxbp

result = rxbp.multicast.zip(
    rxbp.multicast.from_flowable(rxbp.range(4)),
    rxbp.multicast.from_flowable(rxbp.range(4)),
).pipe(
    rxbp.multicast.op.map(lambda v: v[0].zip(v[1]))
).to_flowable().run()

print(result)