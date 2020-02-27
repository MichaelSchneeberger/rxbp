from rxbp.observer import Observer


class ObserverInfo:
    """ A class holding information about "observing" an Observable.

    This class is used as single argument to `observe` method defined in an Observable as it let's us share_flowable it
    in a later phase if needed without changing the Observable interface.
    """

    def __init__(self, observer: Observer, is_volatile: bool = None):
        self.observer = observer
        self.is_volatile = is_volatile if is_volatile is not None else False

    def copy(self, observer: Observer):
        return ObserverInfo(observer=observer, is_volatile=self.is_volatile)
