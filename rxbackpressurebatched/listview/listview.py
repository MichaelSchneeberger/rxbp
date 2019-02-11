from typing import List


class ListView:
    def __init__(self, buffer: List, start: int, length: int):
        self.buffer = buffer

    def __iter__(self):
        return (range(length))