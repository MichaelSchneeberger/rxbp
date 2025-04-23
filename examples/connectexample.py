
import rxbp

c = rxbp.connectable(0, init=0)
s = rxbp.zip((
    c,
    rxbp.from_iterable(range(3)),
)).share()
connections = {c: s}
result = s.run(connections=connections)

print(result)