from typing import Dict, Optional

from rxbp.observable import Observable
from rxbp.selectors.bases import Base
from rxbp.selectors.getselectormixin import GetSelectorMixin, NoSelectorFound, SelectorResult, SelectorFound, \
    ObservableSelector
from rxbp.selectors.selectionop import merge_selectors
from rxbp.subscriber import Subscriber


class SubscriptionInfo(GetSelectorMixin):
    def __init__(
            self,
            base: Optional[Base],
            selectors: Dict[Base, Observable] = None,
    ):
        self.base = base
        self.selectors = selectors

    def get_selectors(
            self,
            other: 'SubscriptionInfo',
            subscriber: Subscriber,
    ) -> SelectorResult:

        if self.base is not None and other.base is not None:
            result = self.base.get_selectors(other.base, subscriber=subscriber)

            if isinstance(result, SelectorFound):
                return result

        def find_selectors(base: Base, selectors: Dict[Base, Observable]):
            for other_base, current_observable in selectors.items():
                result = base.get_selectors(other_base, subscriber=subscriber)

                if isinstance(result, SelectorFound):
                    left_sel = result.left

                    if isinstance(result.right, ObservableSelector):
                        right_sel = ObservableSelector(merge_selectors(
                            result.right.observable,
                            current_observable,
                            scheduler=subscriber.scheduler,
                        ))
                    else:
                        right_sel = ObservableSelector(current_observable)

                    return SelectorFound(left=left_sel, right=right_sel)

            return NoSelectorFound()

        if self.base is not None and other.selectors is not None:
            result = find_selectors(self.base, other.selectors)

            if isinstance(result, SelectorFound):
                return SelectorFound(left=result.right, right=result.left)

        if other.base is not None and self.selectors is not None:
            result = find_selectors(other.base, self.selectors)

            if isinstance(result, SelectorFound):
                return result

        return NoSelectorFound()


class Subscription:
    def __init__(
            self,
            info: SubscriptionInfo,
            observable: Observable,
    ):
        self.info = info
        self.observable = observable
