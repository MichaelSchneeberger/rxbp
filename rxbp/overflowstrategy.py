from abc import ABC


class OverflowStrategy(ABC):
    pass


class BackPressure(OverflowStrategy):
    def __init__(self, buffer_size: int):
        self.buffer_size = buffer_size
