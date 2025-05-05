import rxbp

flowable = rxbp.merge((
    rxbp.from_iterable(range(5)),
    rxbp.from_iterable(('a', 'b', 'c')),
))

result = rxbp.run(flowable)

print(result)
