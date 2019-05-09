from abc import ABC, abstractmethod
from typing import Any, Tuple, List


class SourceMixin(ABC):
    @abstractmethod
    def equals(self, other: 'Base') -> bool:
        ...


class Base(ABC):
    # @abstractmethod
    def is_matching(self, other: 'Base') -> bool:
        source1 = self.get_base_sequence()[0]
        source2 = other.get_base_sequence()[0]

        assert isinstance(source1, SourceMixin) and isinstance(source2, SourceMixin), \
            'first element in base sequence must be a Source'

        return source1.equals(source2)

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

    @abstractmethod
    def _sel_auto_match(self, other: 'Base') -> bool:
        ...

    @abstractmethod
    def sel_auto_match(self, other: 'Base') -> bool:
        ...


class NumericalBase(SourceMixin, Base):
    def __init__(self, num: int):
        self.num = num

    def equals(self, other: Base):
        if isinstance(other, NumericalBase) and self.num == other.num:
            return True
        else:
            return False

    def get_base_sequence(self) -> List['Base']:
        return [self]

    @property
    def buffered(self) -> bool:
        return False

    @property
    def fan_out(self) -> bool:
        return False

    def _sel_auto_match(self, other: 'Base') -> bool:
        return False

    def sel_auto_match(self, other: 'Base') -> bool:
        return other._sel_auto_match(other=self)


class ObjectRefBase(SourceMixin, Base):
    def __init__(self, obj: Any = None):
        self.obj = obj or self

    def equals(self, other: Base):
        if isinstance(other, ObjectRefBase) and self.obj == other.obj:
            return True
        else:
            return False

    def get_base_sequence(self) -> List['Base']:
        return [self]

    @property
    def buffered(self) -> bool:
        return False

    @property
    def fan_out(self) -> bool:
        return False

    def _sel_auto_match(self, other: 'Base') -> bool:
        return False

    def sel_auto_match(self, other: 'Base') -> bool:
        return other._sel_auto_match(other=self)


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

    def _sel_auto_match(self, other: 'Base') -> bool:
        seq1 = self.get_base_sequence()
        seq2 = other.get_base_sequence()

        if 1 < len(seq2):
            def gen_rest():
                i1 = iter(seq1)
                i2 = iter(seq2)

                last1 = None

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

                        # split point found
                        if e1 != e2:
                            yield last1, [e1] + list(i1) + [e2] + list(i2)
                            break

                        last1 = e1
                    elif has_e1:
                        yield last1, [e1] + list(i1) + list(i2)
                    elif has_e1:
                        yield last1, list(i1) + [e2] + list(i2)
                    else:
                        yield last1, list(i1) + list(i2)

            split_point, rest = next(gen_rest())

            # is the split point a base allowing "fan out"
            fan_out = split_point.fan_out

            # in case of "fan out" or buffered node, do not select auto_match
            buffered = any(e.buffered for e in rest)

            return not (fan_out or buffered)
        else:
            return True

    def sel_auto_match(self, other: 'Base') -> bool:
        return self._sel_auto_match(other=other)


class PairwiseBase(Base, SourceMixin):
    def __init__(self, underlying: Base):
        self.underlying = underlying

    def equals(self, other: Base):
        if isinstance(other, PairwiseBase):
            self_source = self.underlying.get_base_sequence()[0]
            other_source = other.underlying.get_base_sequence()[0]

            assert isinstance(self_source, SourceMixin) and isinstance(other_source, SourceMixin), \
                'first element in base sequence must be a Source'

            return self_source.equals(other_source)
        else:
            return False

    def get_base_sequence(self) -> List[Base]:
        return [self]

    @property
    def buffered(self) -> bool:
        return False

    @property
    def fan_out(self) -> bool:
        return False

    def _sel_auto_match(self, other: 'Base') -> bool:
        return False

    def sel_auto_match(self, other: 'Base') -> bool:
        return other._sel_auto_match(other=self)
