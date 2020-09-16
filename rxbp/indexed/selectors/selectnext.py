from dataclasses import dataclass

from rxbp.indexed.selectors.selectmessage import SelectMessage


@dataclass(frozen=True)
class SelectNext(SelectMessage):
    pass


select_next = SelectNext()
