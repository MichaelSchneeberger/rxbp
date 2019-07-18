from rxbp.observer import Observer


class ObserveSubscription:
    def __init__(self, observer: Observer, is_volatile: bool = None):
        self.observer = observer
        self.is_volatile = is_volatile if is_volatile is not None else False

    def copy(self, observer: Observer):
        return ObserveSubscription(observer=observer, is_volatile=self.is_volatile)
