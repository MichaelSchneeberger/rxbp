import rxbp

# create a Flowable source that emits the `init` as the first item and
# then emits the items specified by the connections.
c = rxbp.connectable(0, init=0)

s = rxbp.zip((
    c,
    rxbp.from_iterable(range(3)),
)).share()

result = rxbp.run(
    source=s, 
    connections={c: s},
)

# Prints a list containing 3 elements:
# [(0, 0), ((0, 0), 1), (((0, 0), 1), 2)]
print(result)
