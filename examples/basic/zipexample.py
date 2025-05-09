import rxbp

flowable = rxbp.zip((
    rxbp.from_iterable(range(5)).repeat_first(),
    rxbp.from_iterable(range(3)),
))

result = rxbp.run(flowable)
print(result)