"""
This example uses the match operator to match a double filtered Flowable
with its original sequence.

Two Flowables match if there exists a mapping that maps the elements from
one Flowable to the elements of the other. The maps are defined by operators.
The filter operator defines a map that maps a unfiltered sequence of the
same original base to the filtered version. In this example, the map used
in the match operator is a composition of two maps each defined in a filter
operator.

The first map takes every second element:
0 -> don't take     # element 0
1 -> take           # element 1
2 -> don't take     # element 2
3 -> take           # element 3
4 -> ...

The second map composed with the first map look like this:
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
