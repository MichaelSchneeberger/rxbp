from dataclasses import dataclass
from traceback import FrameSummary
from typing import List

from rxbp.indexed.indexedsubscription import IndexedSubscription
from rxbp.indexed.mixins.indexedflowablemixin import IndexedFlowableMixin
from rxbp.indexed.selectors.flowablebase import FlowableBase
from rxbp.indexed.selectors.flowablebaseandselectors import FlowableBaseAndSelectors, FlowableBaseAndSelectorsMatch
from rxbp.indexed.selectors.identityseqmapinfo import IdentitySeqMapInfo
from rxbp.indexed.selectors.observableseqmapinfo import ObservableSeqMapInfo
from rxbp.indexed.selectors.selectionop import select_observable
from rxbp.observables.zipobservable import ZipObservable
from rxbp.subscriber import Subscriber
from rxbp.utils.tooperatorexception import to_operator_exception


@dataclass
class MatchIndexedFlowable(IndexedFlowableMixin):
    left: IndexedFlowableMixin
    right: IndexedFlowableMixin
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
                sel_left_obs = left_subscription.observable

            # left Flowable needs a transformation to match the other Flowable
            elif isinstance(result.left, ObservableSeqMapInfo):
                left_selector = result.left.observable

                sel_left_obs = select_observable(
                    obs=left_subscription.observable,
                    selector=left_selector,
                    scheduler=subscriber.scheduler,
                    stack=self.stack,
                )
            else:
                raise Exception(to_operator_exception(
                    message=f'illegal selector "{result.left}"',
                    stack=self.stack,
                ))

            if isinstance(result.right, IdentitySeqMapInfo):
                sel_right_obs = right_subscription.observable

            elif isinstance(result.right, ObservableSeqMapInfo):
                right_selector = result.right.observable

                sel_right_obs = select_observable(
                    obs=right_subscription.observable,
                    selector=right_selector,
                    scheduler=subscriber.scheduler,
                    stack=self.stack,
                )
            else:
                raise Exception(to_operator_exception(
                    message=f'illegal selector "{result.right}"',
                    stack=self.stack,
                ))

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

            raise Exception(to_operator_exception(
                message=f'bases do not match "{left_base_name}" and "{right_base_name}"',
                stack=self.stack,
            ))

        observable = ZipObservable(
            left=sel_left_obs,
            right=sel_right_obs,
            stack=self.stack,
        )

        return left_subscription.copy(
            index=FlowableBaseAndSelectors(base=base, selectors=selectors),
            observable=observable,
        )
