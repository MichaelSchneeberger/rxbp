from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, Optional, List

from rxbp.flowables.init.initdebugflowable import init_debug_flowable
from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.selectors.selectionop import select_observable
from rxbp.observables.debugobservable import DebugObservable
from rxbp.observables.init.initdebugobservable import init_debug_observable
from rxbp.observables.zipobservable import ZipObservable
from rxbp.indexed.selectors.flowablebase import FlowableBase
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors, FlowableBaseAndSelectorsMatch
from rxbp.indexed.selectors.observableseqmapinfo import ObservableSeqMapInfo
from rxbp.indexed.selectors.identityseqmapinfo import IdentitySeqMapInfo
from rxbp.subscriber import Subscriber


@dataclass
class MatchIndexedFlowable(IndexedFlowableMixin):
    left: IndexedFlowableMixin
    right: IndexedFlowableMixin
    left_debug: Optional[str]
    right_debug: Optional[str]
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: Subscriber) -> IndexedSubscription:
        left_subscription = self.left.unsafe_subscribe(subscriber=subscriber)
        right_subscription = self.right.unsafe_subscribe(subscriber=subscriber)

        result = left_subscription.index.match_with(
            right_subscription.index,
            subscriber=subscriber,
            stack=self.stack,
        )

        # The resulting matched Flowable propagates selectors of left or right downstream if
        # * both bases match, or
        # * the base of one Flowable matches some selector of the other Flowable
        if isinstance(result, FlowableBaseAndSelectorsMatch):

            # base and selectors of resulting Flowable
            base = result.base_selectors.base
            selectors = result.base_selectors.selectors

            # left Flowable needs no transformation to match the other Flowable
            # the resulting Flowable has base and selectors of left Flowable
            if isinstance(result.left, IdentitySeqMapInfo):
                # base = left_subscription.info.base
                # if left_subscription.info.selectors is not None:
                #     selectors = {**selectors, **left_subscription.info.selectors}
                sel_left_obs = left_subscription.observable

            # left Flowable needs a transformation to match the other Flowable
            elif isinstance(result.left, ObservableSeqMapInfo):
                # if self.left_debug:
                #     left_selector = init_debug_observable(
                #         source=result.left.observable,
                #         name=self.right_debug,
                #         subscriber=subscriber,
                #         stack=self.stack,
                #     )
                # else:
                left_selector = result.left.observable

                sel_left_obs = select_observable(
                    left_subscription.observable,
                    left_selector,
                    scheduler=subscriber.scheduler,
                )
            else:
                raise Exception(f'illegal selector "{result.left}"')

            if isinstance(result.right, IdentitySeqMapInfo):
                # base = right_subscription.info.base
                # if right_subscription.info.selectors is not None:
                #     selectors = {**selectors, **right_subscription.info.selectors}
                sel_right_obs = right_subscription.observable

            elif isinstance(result.right, ObservableSeqMapInfo):
                # if self.right_debug:
                #     right_selector = init_debug_observable(
                #         source=result.right.observable,
                #         name=self.right_debug,
                #         subscriber=subscriber,
                #         stack=self.stack,
                #     )
                # else:
                right_selector = result.right.observable

                sel_right_obs = select_observable(
                    right_subscription.observable,
                    right_selector,
                    scheduler=subscriber.scheduler,
                )
            else:
                raise Exception(f'illegal selector "{result.right}"')

        # if no selector found, raise an Exception
        else:
            if isinstance(left_subscription.index.base, FlowableBase):
                left_base_name = left_subscription.index.base.get_name()
            else:
                left_base_name = 'None'

            if isinstance(right_subscription.index.base, FlowableBase):
                right_base_name = right_subscription.index.base.get_name()
            else:
                right_base_name = 'None'

            raise Exception(f'bases do not match of "{left_base_name}" and "{right_base_name}"')

        observable = ZipObservable(
            left=sel_left_obs,
            right=sel_right_obs,
            stack=self.stack,
        )

        return left_subscription.copy(
            index=FlowableBaseAndSelectors(base=base, selectors=selectors),
            observable=observable,
        )
