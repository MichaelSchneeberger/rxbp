from donotation import do

import continuationmonad
from continuationmonad.typing import Scheduler

import rxbp
from rxbp.typing import Observer


@do()
def delay_second_item(observer: Observer, scheduler: Scheduler):
    yield observer.on_next(1)
    yield continuationmonad.sleep(1, scheduler)
    return observer.on_next_and_complete(2)

f = (
    rxbp.create(delay_second_item)
    .tap(print)
)

result = rxbp.run(f)

# Prints: [1, 2]
print(result)
