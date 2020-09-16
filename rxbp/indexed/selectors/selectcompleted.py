from dataclasses import dataclass

from rxbp.indexed.selectors.selectmessage import SelectMessage


@dataclass(frozen=True)
class SelectCompleted(SelectMessage):
    pass


select_completed = SelectCompleted()