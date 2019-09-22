from typing import Callable, Any, Union, List, Dict, Tuple

import rxbp
from rxbp.flowable import Flowable


class MapToDictOperator:
    def __init__(self, func: Callable[[Any], Tuple[Flowable, Dict[Any, Callable[[Flowable], Flowable]]]]):
        self.func = func

    def __call__(self, obj: Any) -> Flowable:
        out_flow, out_dict = self.func(obj)

        return out_flow.pipe(
            rxbp.op.share(lambda f: rxbp.zip([
                rxbp.return_value(map_func(f)).map(lambda v, k=k: (k, v)) for k, map_func in out_dict.items()
            ], lambda *v: dict(v))),
        )
