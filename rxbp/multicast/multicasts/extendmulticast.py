from typing import Callable

import rx
from rx import operators as rxop
from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.typing import MultiCastValue


class ExtendMultiCast(MultiCastBase):
    def __init__(
            self,
            source: MultiCastBase,
            func: Callable[[MultiCastValue], Flowable],
    ):
        self.source = source
        self.func = func

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        def selector(prev_base: MultiCastValue, val: MultiCastValue):
            """
            List + Any  => List
            List + List => List
            Dict + Dict => Dict
            ? + Any     => Any
            """

            def to_ref_count_flowable(flowable: FlowableBase):
                return Flowable(RefCountFlowable(flowable))

            if isinstance(val, Flowable):
                shared_val = to_ref_count_flowable(val)
            elif isinstance(val, list):
                shared_val = [to_ref_count_flowable(e) for e in val]

                if isinstance(prev_base, list):
                    return prev_base + shared_val
            elif isinstance(val, dict):
                shared_val = {k: to_ref_count_flowable(v) for k, v in val.items()}

                if isinstance(prev_base, dict):
                    return {**prev_base, **shared_val}
            elif isinstance(val, FlowableStateMixin):
                state = val.get_flowable_state()
                new_state = {k: to_ref_count_flowable(v) for k, v in state.items()}
                shared_val = val.set_flowable_state(new_state)

                if isinstance(prev_base, dict):
                    return {**prev_base, **shared_val}
            else:
                raise Exception(f'îllegal value "{val}"')

            return shared_val

        def map_func(base: MultiCastValue):
            return selector(base, self.func(base))

        source = self.source.get_source(info=info).pipe(
            rxop.map(map_func),
        )

        return source