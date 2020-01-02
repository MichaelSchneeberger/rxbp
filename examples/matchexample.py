"""
This example applies a more complicated map to match two Flowables. In fact,
this map is defined as a composition of two simple maps. Remember that two
Flowables match if the elements from one Flowable the elements of the other.

The first mapping that matches a base of 10 takes every second element:
0 -> don't take     # element 0
1 -> take           # element 1
2 -> don't take     # element 2
3 -> take           # element 3
4 -> ...

The second mapping that get composes with the first look like this:
0 -> take           # element 1
1 -> take           # element 3
2 -> don't take     # element 5
3 -> take           # element 7
4 -> take           # element 9

"""

import rxbp

f1 = rxbp.range(10)

f2 = rxbp.range(10).pipe(
    rxbp.op.filter(lambda v: v%2),          # first mapping is defined in f2
)

f3 = rxbp.match(f1, f2).pipe(               # apply first mapping to f1
    rxbp.op.filter(lambda v: v[0]%5),       # second mapping composed with first mapping is defined in f3
)

f4 = rxbp.range(10)

result = rxbp.match(f3, f4).run()           # apply composed mapping to f4

print(result)
