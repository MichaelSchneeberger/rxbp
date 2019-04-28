from abc import ABC


class OverflowStrategy(ABC):
    pass


class BackPressure(OverflowStrategy):
    # unbounded buffer

    def __init__(self, buffer_size: int):
        self.buffer_size = buffer_size
