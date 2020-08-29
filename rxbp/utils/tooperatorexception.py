from traceback import FrameSummary
from typing import List


def to_operator_exception(
        message: str,
        stack: List[FrameSummary],
) -> str:
    exception_lines = [
        message,
        f'  Traceback rxbp (most recent call last):',
        *(f'    File "{stack_line.filename}", line {stack_line.lineno}\n      {stack_line.line}' for stack_line in stack),
    ]

    return '\n'.join(exception_lines)
