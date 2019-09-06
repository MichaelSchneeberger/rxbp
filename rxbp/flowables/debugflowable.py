from rxbp.flowablebase import FlowableBase
from rxbp.subscriber import Subscriber
from rxbp.testing.debugobservable import DebugObservable


class DebugFlowable(FlowableBase):
    def __init__(
            self,
            source: FlowableBase,
            name=None, on_next=None, on_subscribe=None, on_ack=None, on_raw_ack=None, on_ack_msg=None,
    ):
        super().__init__(base=source.base, selectable_bases=source.selectable_bases)

        self._source = source
        self._name = name
        self._on_next = on_next
        self._on_subscribe = on_subscribe
        self._on_ack = on_ack
        self._on_raw_ack = on_raw_ack
        self._on_ack_msg = on_ack_msg

    def unsafe_subscribe(self, subscriber: Subscriber):
        source_obs, selector = self._source.unsafe_subscribe(subscriber=subscriber)

        obs = DebugObservable(source=source_obs, name=self._name, on_next=self._on_next, on_subscribe=self._on_subscribe,
                              on_ack=self._on_ack, on_raw_ack=self._on_raw_ack)

        return obs, selector