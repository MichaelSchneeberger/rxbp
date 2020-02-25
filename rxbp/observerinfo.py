from rxbp.observer import Observer


class ObserverInfo:
    """
    Information about "observing" an Observable. ObserverInfo is used as
    single argument to `observe` method defined in an Observable.
    """

    def __init__(
            self,
            observer: Observer,

            # this argument only has an effect on shared Flowables. If set
            # to True, then a shared Flowables completes once there is no
            # more non-volatile observers
            is_volatile: bool = None,
    ):
        self.observer = observer

        if is_volatile is None:
            self.is_volatile = False
        else:
            self.is_volatile = is_volatile

    def copy(self, observer: Observer):
        return ObserverInfo(observer=observer, is_volatile=self.is_volatile)
