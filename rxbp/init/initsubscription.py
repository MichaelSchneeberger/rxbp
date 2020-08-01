from rxbp.impl.subscriptionImpl import SubscriptionImpl
from rxbp.observable import Observable


def init_subscription(
    observable: Observable,
):
    return SubscriptionImpl(
        observable=observable,
    )
