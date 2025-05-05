from donotation import do

import rxbp

@do()
def create_flowable():
    num = yield rxbp.from_iterable(range(4))

    v = rxbp.from_iterable(range(num)).share()
    return rxbp.zip((v, v))


result = rxbp.run(create_flowable())

# Prints: [(0, 0), (0, 0), (1, 1), (0, 0), (1, 1), (2, 2)]
print(result)
