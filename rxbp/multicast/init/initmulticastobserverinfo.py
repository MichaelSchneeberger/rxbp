from rxbp.multicast.multicastobserver import MultiCastObserver
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo


def init_multicast_observer_info(
        observer: MultiCastObserver,
):
    return MultiCastObserverInfo(
        observer=observer,
    )
