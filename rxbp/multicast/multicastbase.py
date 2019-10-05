from abc import ABC, abstractmethod
from dataclasses import dataclass

from typing import Generic, Union

import rxbp

from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.scheduler import Scheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType
from rxbp.multicast.typing import MultiCastValue


class MultiCastBase(Generic[MultiCastValue], ABC):
    @dataclass
    class LiftedFlowable:
        source: Flowable

    class MultiCastInfo:
        def __init__(self, subscribe_scheduler: Scheduler):
            self.subscribe_scheduler = subscribe_scheduler

    @abstractmethod
    def get_source(self, info: MultiCastInfo) -> Flowable:
        ...

    def to_flowable(self) -> Flowable[ValueType]:
        source = self

        class MultiCastToFlowable(FlowableBase):
            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:

                def flat_map_func(v: MultiCastValue):
                    if isinstance(v, MultiCastBase.LiftedFlowable):
                        return v.source.pipe()
                    else:
                        return v

                info = MultiCastBase.MultiCastInfo(subscribe_scheduler=subscriber.subscribe_scheduler)

                return source.get_source(info=info).pipe(
                    rxbp.op.filter(lambda v: isinstance(v, MultiCastBase.LiftedFlowable) or isinstance(v, Flowable)),
                    rxbp.op.first(raise_exception=lambda f: f()),
                    rxbp.op.subscribe_on(),
                    rxbp.op.flat_map(flat_map_func),
                ).unsafe_subscribe(subscriber=subscriber)

        return Flowable(MultiCastToFlowable())


MultiCastFlowable = Union[MultiCastBase.LiftedFlowable, Flowable]

