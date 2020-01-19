from abc import ABC, abstractmethod
from typing import Generic

import rx
from rx import operators as rxop

import rxbp
from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowables.subscribeonflowable import SubscribeOnFlowable
from rxbp.multicast.flowabledict import FlowableDict
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.typing import MultiCastValue
from rxbp.schedulers.trampolinescheduler import TrampolineScheduler
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.typing import ValueType


class MultiCastBase(Generic[MultiCastValue], ABC):
    @abstractmethod
    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable[MultiCastValue]:
        ...

    def to_flowable(self) -> Flowable[ValueType]:
        source = self

        class MultiCastToFlowable(FlowableBase):
            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:

                def flat_map_func(v: MultiCastValue):
                    if isinstance(v, Flowable):
                        return v
                    elif isinstance(v, list):
                        flist = [f.to_list() for f in v if isinstance(f, Flowable)]
                        return rxbp.zip(*flist)
                    elif isinstance(v, dict) or isinstance(v, FlowableDict):
                        if isinstance(v, dict):
                            fdict = v
                        else:
                            fdict = v.get_flowable_state()

                        # keys, flist = zip(*((key, f) for key, f in fdict.items() if isinstance(f, Flowable)))
                        # return flist[0]

                        keys, flist = zip(*((key, f.to_list()) for key, f in fdict.items() if isinstance(f, Flowable)))
                        return rxbp.zip(*flist).pipe(
                            # rxbp.op.debug(('d1')),
                            rxbp.op.map(lambda vlist: dict(zip(keys, vlist))),
                        )
                    else:
                        raise Exception(f'illegal value "{v}"')

                scheduler = TrampolineScheduler()

                info = MultiCastInfo(
                    source_scheduler=subscriber.subscribe_scheduler,
                    multicast_scheduler=scheduler,
                )

                source_flowable = rxbp.from_rx(
                    source.get_source(info=info).pipe(
                        rxop.filter(lambda v: any([
                            isinstance(v, Flowable),
                            isinstance(v, list) and any(isinstance(e, FlowableBase) for e in v),# and len(v) == 1,
                            isinstance(v, dict) and any(isinstance(e, FlowableBase) for e in v.values()),# and len(v) == 1,
                            isinstance(v, FlowableDict) and any(isinstance(e, FlowableBase) for e in v.get_flowable_state().values()),# and len(v) == 1,
                        ])),
                        rxop.first(),
                ))
                return Flowable(SubscribeOnFlowable(source_flowable, scheduler=info.multicast_scheduler)).pipe(
                    rxbp.op.flat_map(flat_map_func),
                ).unsafe_subscribe(subscriber=subscriber)

        return Flowable(MultiCastToFlowable())
