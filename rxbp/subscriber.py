from abc import ABC

from rxbp.mixins.subscribermixin import SubscriberMixin


class Subscriber(SubscriberMixin, ABC):
    pass

    # def return_value(self, val: ValueType) -> SubscriptionMixin:
    #     return init_subscription(
    #         observable=ReturnValueObservable(
    #             val=val,
    #             subscribe_scheduler=self.subscribe_scheduler,
    #         ),
    #     )
