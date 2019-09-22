from typing import Callable, Any, Union, List, Dict

import rxbp
from rxbp.flowable import Flowable
from rxbp.typing import BaseType


class MapOperator:
    def __init__(self, func: Callable[[BaseType], Flowable]):
        self.func = func

    def __call__(self, obj: Any) -> Flowable:
        result = self.func(obj)

        if isinstance(result, Flowable):
            return self.func(obj).share(lambda f: rxbp.return_value(f))

        elif isinstance(result, list):
            assert all(isinstance(e, Flowable) for e in result)

            def func(*flowables):
                delayed_flowables = [rxbp.return_value(f) for f in flowables]
                return rxbp.zip(delayed_flowables, lambda *v: list(v))

            return rxbp.share(sources=result, func=func)

            # shared_result = [e.share(lambda f: rxbp.return_value(f)) for e in result]
            # return rxbp.zip(shared_result)

        elif isinstance(result, dict):
            assert all(isinstance(e, Flowable) for e in result.values())

            keys, sources = zip(*result.items())

            def func(*flowables):
                delayed_flowables = [rxbp.return_value(f).map(lambda v, k=k: (k, v)) for k, f in zip(keys, flowables)]
                return rxbp.zip(delayed_flowables, lambda *v: dict(v))

            return rxbp.share(sources=sources, func=func)

            # shared_result = [v.share(lambda f, k=k: rxbp.return_value(f).map(lambda v: (k, v))) for k, v in result.items()]
            # return rxbp.zip(shared_result, lambda *v: dict(v))

        else:
            raise Exception('illegal return value "{}"'.format(result))
