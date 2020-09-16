from typing import Optional, Any

from rxbp.indexed.selectors.bases.numericalbase import NumericalBase
from rxbp.indexed.selectors.bases.objectrefbase import ObjectRefBase
from rxbp.indexed.selectors.flowablebase import FlowableBase


def init_base(base: Optional[Any]) -> FlowableBase:
    if base is not None:
        if isinstance(base, str):
            base = ObjectRefBase(base)
        elif isinstance(base, int):
            base = NumericalBase(base)
        elif isinstance(base, FlowableBase):
            base = base
        else:
            raise Exception(f'illegal base "{base}"')

    return base