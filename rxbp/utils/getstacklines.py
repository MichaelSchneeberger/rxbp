import traceback

from traceback import FrameSummary
from typing import List


def get_stack_lines(index: int = 2) -> List[FrameSummary]:
    stack_lines = traceback.extract_stack()[:-index]
    return stack_lines
