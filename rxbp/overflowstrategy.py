from abc import ABC


class OverflowStrategy(ABC):
    def __init__(self, buffer_size: int):
        self.buffer_size = buffer_size


class BackPressure(OverflowStrategy):
    # unbounded buffer

    # def __init__(self, buffer_size: int):
    #     self.buffer_size = buffer_size
    pass


class DropOld(OverflowStrategy):
    # unbounded buffer

    # def __init__(self, buffer_size: int):
    #     self.buffer_size = buffer_size
    pass


class ClearBuffer(OverflowStrategy):
    # unbounded buffer

    # def __init__(self, buffer_size: int):
    #     self.buffer_size = buffer_size
    pass
