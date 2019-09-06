from typing import Callable, Any, Set

from rxbp.flowablebase import FlowableBase, FlowableBase
from rxbp.selectors.selectionop import select_observable
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.selectors.bases import Base
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription
from rxbp.testing.debugobservable import DebugObservable


class MatchFlowable(FlowableBase):
    def __init__(
            self,
            left: FlowableBase,
            right: FlowableBase,
            func: Callable[[Any, Any], Any] = None,
            auto_match: bool = None
    ):
        """
        :param left:
        :param right:
        :param func:
        :param auto_match: if set to False then this Flowable works like a normal zip operation, if set to False then \
        it checks if the left and right Flowable either match (by their corresponding bases) or there is a \
        transformation (called selector) to make them match
        """

        super().__init__()

        self._left = left
        self._right = right
        self._func = func
        self._auto_match = auto_match

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        left_subscription = self._left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self._right.unsafe_subscribe(subscriber=subscriber)

        # print(left_subscription)
        # print(right_subscription)

        # if left_subscription.base.matches(right_subscription.base):
        #     observable = Zip2Observable(
        #         left=left_subscription.observable,
        #         right=right_subscription.observable,
        #         selector=self._func,
        #     )
        #     selectors = {**left_subscription.selectors, **right_subscription.selectors}
        #
        #     return Subscription(base=left_subscription.base, observable=observable, selectors=selectors)
        #
        # # transform right Flowable to match base of left Flowable
        # if left_obs is not None:
        #     left_obs_ = select_observable(left_obs, left_index_obs, scheduler=subscriber.scheduler)
        # else:
        #     selectors = {**selectors, **left_selectors}
        #     left_obs_ = left_obs
        #
        # # transform left Flowable to match base of right Flowable
        # if right_index_obs is not None:
        #     right_obs_ = select_observable(right_obs, right_index_obs, scheduler=subscriber.scheduler)
        # else:
        #     selectors = {**selectors, **right_selectors}
        #     right_obs_ = right_obs

        result = left_subscription.info.get_selectors(right_subscription.info, subscriber=subscriber)

        print(result)

        obs = Zip2Observable(left=left_obs_, right=right_obs_, selector=self._func)

        return Subscription()
