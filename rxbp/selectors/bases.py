from abc import ABC, abstractmethod
from typing import Any, Tuple, List


class Base(ABC):
    @abstractmethod
    def is_matching(self, other: 'Base') -> Tuple[bool, bool]:
        ...

    @abstractmethod
    def get_base_sequence(self) -> List['Base']:
        ...

    @property
    @abstractmethod
    def buffered(self) -> bool:
        ...

    @property
    @abstractmethod
    def fan_out(self) -> bool:
        ...


class NumericalBase(Base):
    def __init__(self, num: int):
        self.num = num

    def is_matching(self, other: Base):
        if isinstance(other, NumericalBase) and self.num == other.num:
            return True, False
        else:
            return False, False

    def get_base_sequence(self) -> List['Base']:
        return [self]

    @property
    def buffered(self) -> bool:
        return False

    @property
    def fan_out(self) -> bool:
        return False


class ObjectRefBase(Base):
    def __init__(self, obj: Any = None):
        self.obj = obj or self

    def is_matching(self, other: Base):
        if isinstance(other, ObjectRefBase) and self.obj == other.obj:
            return True, False
        else:
            return False, False

    def get_base_sequence(self) -> List['Base']:
        return [self]

    @property
    def buffered(self) -> bool:
        return False

    @property
    def fan_out(self) -> bool:
        return False


class SharedBase(Base):
    def __init__(self, has_fan_out: bool, prev_base: Base = None):
        self.has_fan_out = has_fan_out
        self.prev_base = prev_base or ObjectRefBase(self)

    def get_base_sequence(self) -> List['Base']:
        return self.prev_base.get_base_sequence() + [self]

    @property
    def buffered(self) -> bool:
        return False

    @property
    def fan_out(self) -> bool:
        return not self.has_fan_out

    def is_matching(self, other: Base):
        seq1 = self.get_base_sequence()
        seq2 = other.get_base_sequence()

        is_matching, _ = seq1[0].is_matching(seq2[0])

        if is_matching and 1 < len(seq1) and 1 < len(seq2):
            def gen_rest():
                i1 = iter(seq1)
                i2 = iter(seq2)

                last1 = None
                last2 = None

                while True:
                    try:
                        e1 = next(i1)
                        has_e1 = True
                    except StopIteration:
                        e1 = None
                        has_e1 = False

                    try:
                        e2 = next(i2)
                        has_e2 = True
                    except StopIteration:
                        e2 = None
                        has_e2 = False

                    if has_e1 and has_e2:
                        if e1 != e2:
                            yield last1, [e1] + list(i1) + [e2] + list(i2)
                            break

                        last1 = e1
                        last2 = e2
                    elif has_e1:
                        yield last1, [e1] + list(i1) + list(i2)
                    elif has_e1:
                        yield last1, list(i1) + [e2] + list(i2)
                    else:
                        yield last1, list(i1) + list(i2)

            rest1, rest2 = next(gen_rest())

            fan_out = rest1.fan_out
            buffered = any(e.buffered for e in rest2)

            return is_matching, not (fan_out or buffered)
        else:
            return is_matching, False
