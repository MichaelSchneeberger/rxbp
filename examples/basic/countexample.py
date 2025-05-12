import rxbp


def flowable_of_unknown_size():
    return rxbp.from_(iter('Hello'))

# implementation of the zip_with_index operator
f = (
    flowable_of_unknown_size()
    .zip((rxbp.count(),))
)

result = rxbp.run(f)

# Prints: [('H', 0), ('e', 1), ('l', 2), ('l', 3), ('o', 4)]
print(result)
