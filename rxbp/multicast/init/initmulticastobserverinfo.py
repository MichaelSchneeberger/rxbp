from rxbp.multicast.impl.multicastobserverinfoimpl import MultiCastObserverInfoImpl
from rxbp.multicast.multicastobserver import MultiCastObserver


def init_multicast_observer_info(
        observer: MultiCastObserver,
):
    return MultiCastObserverInfoImpl(
        observer=observer,
    )
