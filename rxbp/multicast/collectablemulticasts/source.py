import rxbp
from rxbp.multicast.collectablemulticasts.collectablemulticast import CollectableMultiCast
from rxbp.multicast.multicast import MultiCast


class CollectableMultiCastSource:
    @staticmethod
    def from_multicast(val: MultiCast):
        return CollectableMultiCast(main=val, collected=rxbp.multicast.empty())
