from donotation import do
import continuationmonad
import rxbp

# event loop scheduler that is started from the main thread
s1 = continuationmonad.init_main_scheduler()

# event loop started in new thread
s2 = continuationmonad.init_event_loop_scheduler()


@do()
def stream1():
    n = yield from rxbp.from_iterable(['a', 'b', 'c'])
    yield rxbp.sleep(1, s1)
    return rxbp.from_value(n)

@do()
def stream2():
    n = yield from rxbp.from_iterable(range(6))
    yield rxbp.sleep(0.5, s2)
    return rxbp.from_value(n)


flowable = rxbp.merge((
    stream1().tap(print),
    stream2().tap(print),
))

result = rxbp.run(
    source=flowable,
    scheduler=s1,
)

# Print: [0, 'a', 1, 2, 'b', 3, 4, 'c', 5]
print(result)
