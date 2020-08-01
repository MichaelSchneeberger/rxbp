from rxbp.multicast.impl.multicastsubscriptionimpl import MultiCastSubscriptionImpl
from rxbp.multicast.mixins.multicastobservablemixin import MultiCastObservableMixin


def init_multicast_subscription(
        observable: MultiCastObservableMixin,
):
    return MultiCastSubscriptionImpl(observable=observable)

