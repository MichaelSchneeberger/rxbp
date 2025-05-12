from abc import abstractmethod
import traceback

from dataclasses import dataclass
from dataclasses import fields


@dataclass(frozen=True)
class FrameSummary:
    filename: str
    lineno: int | None
    name: str
    line: str | None


def get_frame_summary(index: int = 3):
    def gen_stack_lines():
        for obj in traceback.extract_stack()[:-index]:
            if "<frozen importlib._bootstrap" not in obj.filename:
                yield FrameSummary(
                    filename=obj.filename,
                    lineno=obj.lineno,
                    name=obj.name,
                    line=obj.line,
                )

    return tuple(gen_stack_lines())


def to_operator_traceback(stack: tuple[FrameSummary, ...]):
    assert stack is not None

    traceback_line = (
        *(
            f'  File "{stack_line.filename}", line {stack_line.lineno}\n    {stack_line.line}'
            for stack_line in stack
        ),
    )

    return "\n".join(traceback_line)


def to_execution_exception_message(traceback: str):
    message = (
        "An exception caught during execution of the RxBP. "
        "See the traceback below for details on the execution call stack:"
    )
    return f"{message}\n{traceback}"


def to_operator_exception_message(stack: tuple[FrameSummary, ...]):
    message = (
        "RxBP operator exception caught. "
        "See the traceback below for details on the operator call stack:"
    )
    traceback = to_operator_traceback(stack=stack)
    return f"{message}\n{traceback}"


class FrameSummaryMixin:
    @property
    @abstractmethod
    def stack(self) -> tuple[FrameSummary, ...]: ...

    # implement custom __repr__ method that returns a representation without the stack
    def __repr__(self):
        fields_str = ",".join(
            f"{field.name}={repr(getattr(self, field.name))}"
            for field in fields(self)
            if field.name != "stack"
        )  # type: ignore

        return f"{self.__class__.__name__}({fields_str})"

    to_operator_traceback = staticmethod(to_operator_traceback)
    to_execution_exception_message = staticmethod(to_execution_exception_message)
    to_operator_exception_message = staticmethod(to_operator_exception_message)
