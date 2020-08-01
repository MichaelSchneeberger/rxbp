from rxbp.impl.observerinfoimpl import ObserverInfoImpl
from rxbp.observer import Observer


def init_observer_info(
        observer: Observer,
        is_volatile: bool = None,
):
    if is_volatile is None:
        is_volatile = False

    return ObserverInfoImpl(
        observer=observer,
        is_volatile=is_volatile,
    )
