import rxbp


f = (
    rxbp.count()
    .take_while(lambda i: i < 10)
    .batch(size=4)
)

result = rxbp.run(f)

# Prints: [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]
print(result)
