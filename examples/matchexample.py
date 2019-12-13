import rxbp

f1 = rxbp.range(10).pipe(
    rxbp.op.filter(lambda v: v%2),
)

f2 = rxbp.range(10)

f3 = rxbp.match(f1, f2).pipe(
    rxbp.op.filter(lambda v: v[0]%5),
)

result = rxbp.match(f3, f2).run()

print(result)
