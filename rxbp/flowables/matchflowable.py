from typing import Callable, Any

from rxbp.flowablebase import FlowableBase
from rxbp.observables.zip2observable import Zip2Observable
from rxbp.selectors.base import Base
from rxbp.selectors.baseandselectors import BaseAndSelectors, BaseSelectorsAndSelectorMaps
from rxbp.selectors.selectionop import select_observable
from rxbp.selectors.selectormap import IdentitySelectorMap, ObservableSelectorMap
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class MatchFlowable(FlowableBase):
    def __init__(
            self,
            left: FlowableBase,
            right: FlowableBase,
            func: Callable[[Any, Any], Any] = None,
    ):
        """
        :param left:
        :param right:
        :param func:
        """

        super().__init__()

        self._left = left
        self._right = right
        self._func = func

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        left_subscription = self._left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self._right.unsafe_subscribe(subscriber=subscriber)

        result = left_subscription.info.get_selectors(
            right_subscription.info,
            subscriber=subscriber,
        )

        # The resulting matched Flowable propagates selectors of left or right downstream if
        # * both bases match, or
        # * the base of one Flowable matches some selector of the other Flowable
        if isinstance(result, BaseSelectorsAndSelectorMaps):

            # base and selectors of resulting Flowable
            base = result.base_selectors.base
            selectors = result.base_selectors.selectors

            # left Flowable needs no transformation to match the other Flowable
            # the resulting Flowable has base and selectors of left Flowable
            if isinstance(result.left, IdentitySelectorMap):
                # base = left_subscription.info.base
                # if left_subscription.info.selectors is not None:
                #     selectors = {**selectors, **left_subscription.info.selectors}
                sel_left_obs = left_subscription.observable

            # left Flowable needs a transformation to match the other Flowable
            elif isinstance(result.left, ObservableSelectorMap):
                sel_left_obs = select_observable(
                    left_subscription.observable,
                    result.left.observable,
                    scheduler=subscriber.scheduler,
                )
            else:
                raise Exception(f'illegal selector "{result.left}"')

            if isinstance(result.right, IdentitySelectorMap):
                # base = right_subscription.info.base
                # if right_subscription.info.selectors is not None:
                #     selectors = {**selectors, **right_subscription.info.selectors}
                sel_right_obs = right_subscription.observable

            elif isinstance(result.right, ObservableSelectorMap):
                sel_right_obs = select_observable(
                    right_subscription.observable,
                    result.right.observable,
                    scheduler=subscriber.scheduler,
                )
            else:
                raise Exception(f'illegal selector "{result.right}"')

        # if no selector found, raise an Exception
        else:
            if isinstance(left_subscription.info.base, Base):
                left_base_name = left_subscription.info.base.get_name()
            else:
                left_base_name = 'None'

            if isinstance(right_subscription.info.base, Base):
                right_base_name = right_subscription.info.base.get_name()
            else:
                right_base_name = 'None'

            raise Exception(f'bases do not match of "{left_base_name}" and "{right_base_name}"')

        observable = Zip2Observable(
            left=sel_left_obs,
            right=sel_right_obs,
            selector=self._func,
        )

        return Subscription(info=BaseAndSelectors(base=base, selectors=selectors), observable=observable)
