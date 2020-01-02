from rxbp.observable import Observable


class Selector:
    """
    A Selector is a function that maps the elements of a sequence with some base B_1 to the matched base B_matched

    * identity - map all elements one-to-one
    * observableselector - modify sequence by the selector
    """
    pass


class IdentitySelector(Selector):
    pass


class ObservableSelector(Selector):
    def __init__(self, observable: Observable):
        self.observable = observable
