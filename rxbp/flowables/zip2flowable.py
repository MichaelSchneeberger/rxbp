from typing import Callable, Any, Set

from rxbp.flowablebase import FlowableBase, FlowableBase
from rxbp.selectors.getselectormixin import NoSelectorFound, SelectorFound, IdentitySelector, ObservableSelector
from rxbp.selectors.selectionop import select_observable
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.selectors.bases import Base
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo
from rxbp.testing.debugobservable import DebugObservable


class Zip2Flowable(FlowableBase):
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

        if self._auto_match is True:
            result = left_subscription.info.get_selectors(right_subscription.info, subscriber=subscriber)

            if isinstance(result, SelectorFound):
                base = None
                selectors = {}

                if isinstance(result.left, IdentitySelector):
                    base = left_subscription.info.base
                    if left_subscription.info.selectors is not None:
                        selectors = {**selectors, **left_subscription.info.selectors}
                    sel_left_obs = left_subscription.observable
                elif isinstance(result.left, ObservableSelector):
                    sel_left_obs = select_observable(
                        left_subscription.observable,
                        result.left.observable,
                        scheduler=subscriber.scheduler,
                    )
                else:
                    raise Exception('illegal selector "{}"'.format(result.left))

                if isinstance(result.right, IdentitySelector):
                    base = right_subscription.info.base
                    if right_subscription.info.selectors is not None:
                        selectors = {**selectors, **right_subscription.info.selectors}
                    sel_right_obs = right_subscription.observable
                elif isinstance(result.right, ObservableSelector):
                    sel_right_obs = select_observable(
                        right_subscription.observable,
                        result.right.observable,
                        scheduler=subscriber.scheduler,
                    )
                else:
                    raise NotImplementedError

            else:
                left_base_name = left_subscription.info.base.get_name() if isinstance(left_subscription.info.base, Base) else 'None'
                right_base_name = right_subscription.info.base.get_name() if isinstance(right_subscription.info.base, Base) else 'None'
                raise Exception('bases do not match of "{}" and "{}"'.format(left_base_name, right_base_name))

        else:
            sel_left_obs = left_subscription.observable
            sel_right_obs = right_subscription.observable

            result = left_subscription.info.get_selectors(right_subscription.info, subscriber=subscriber)
            if isinstance(result, SelectorFound):
                if isinstance(result.left, IdentitySelector) and isinstance(result.right, IdentitySelector):
                    base = left_subscription.info.base

                    selectors = {}
                    if left_subscription.info.selectors is not None:
                        selectors = {**selectors, **left_subscription.info.selectors}
                    if right_subscription.info.selectors is not None:
                        selectors = {**selectors, **right_subscription.info.selectors}
                else:
                    base = None
                    selectors = None
            else:
                base = None
                selectors = None

        observable = Zip2Observable(left=sel_left_obs, right=sel_right_obs, selector=self._func)

        return Subscription(info=SubscriptionInfo(base=base, selectors=selectors), observable=observable)
