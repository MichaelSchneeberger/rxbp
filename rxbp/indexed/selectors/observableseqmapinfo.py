from dataclasses import dataclass

from dataclass_abc import dataclass_abc

from rxbp.indexed.selectors.mixins.observablemixin import ObservableMixin
from rxbp.indexed.selectors.seqmapinfo import SeqMapInfo
from rxbp.observable import Observable


@dataclass_abc
class ObservableSeqMapInfo(ObservableMixin, SeqMapInfo):
    """
    maps the sequence with the help of an observable emitting selecting
    elements "next" and "complete"
    """

    observable: Observable
