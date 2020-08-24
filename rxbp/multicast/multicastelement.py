from rxbp.multicast.typing import MultiCastItem


class MultiCastElement:
    def __init__(self, val: MultiCastItem, n_lifts: int = 0):
        self.val = val
        self.n_lifts = n_lifts