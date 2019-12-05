from rxbp.multicast.typing import MultiCastValue


class MultiCastElement:
    def __init__(self, val: MultiCastValue, n_lifts: int = 0):
        self.val = val
        self.n_lifts = n_lifts