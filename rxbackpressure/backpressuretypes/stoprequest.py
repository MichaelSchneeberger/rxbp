import math


class StopRequest:
    def __lt__(self, other):
        return False

    def __gt__(self, other):
        return True

    def __add__(self, other):
        return math.inf

    def __radd__(self, other):
        return math.inf