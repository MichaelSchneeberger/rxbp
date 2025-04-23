import rxbp

# Defines a flowable that emits up to 7 elements
range7 = rxbp.from_iterable(range(7))

# Defines a shared flowable that emits up to 5 elements
# The shared operator ensures that the downstream flowable is subscribed once
range5 = rxbp.from_iterable(range(3)).share()

# Zip elements of the three flowables
flowable = rxbp.zip((range5, range7, range5))

# Run flowable and collect received items in a list
result = flowable.run()

#The output will be [(0, 0, 0), (1, 1, 1), (2, 2, 2)]
print(result)