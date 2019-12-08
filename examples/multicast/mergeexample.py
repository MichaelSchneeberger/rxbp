import rxbp

m1 = rxbp.multicast.from_flowable(rxbp.range(10))
m2 = rxbp.multicast.from_flowable(rxbp.range(5))


result = rxbp.multicast.merge(m1, m2).pipe(
    # rxbp.multicast.op.debug('d1'),
    rxbp.multicast.op.reduce(),
).to_flowable().run()

print(result)