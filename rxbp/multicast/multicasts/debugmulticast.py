import rx

from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicastobservables.debugmulticastobservable import DebugMultiCastObservable
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber


# todo: assert subscribe_scheduler is active
from rxbp.multicast.multicastsubscription import MultiCastSubscription


class DebugMultiCast(MultiCastMixin):
    def __init__(
            self,
            source: MultiCastMixin,
            name: str = None,
    ):
        self.source = source
        self.name = name

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        print(f'{self.name}.get_source({subscriber})')

        subscription = self.source.unsafe_subscribe(subscriber=subscriber)
        return subscription.copy(
            observable=DebugMultiCastObservable(
                subscription.observable,
                name=self.name,
                on_next=print,
                on_completed=lambda: print('completed'),
                on_error=print,
            )
        )
