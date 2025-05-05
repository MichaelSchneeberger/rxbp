import rxbp

source = rxbp.from_iterable(("Alpha", "Beta", "Gamma", "Delta", "Epsilon"))

flowable = (
    source
    .map(lambda s: len(s))
    .filter(lambda i: i >= 5)
    .tap(on_next=lambda v: print(f'Received {v}'))
)

# execute the flowable
result = rxbp.run(flowable)
