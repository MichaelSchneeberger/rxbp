from rxbp.multicast.impl.multicastsubscriptionimpl import MultiCastSubscriptionImpl
from rxbp.multicast.multicastobservable import MultiCastObservable


def init_multicast_subscription(
        observable: MultiCastObservable,
):
    return MultiCastSubscriptionImpl(observable=observable)

