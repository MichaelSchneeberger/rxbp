import threading
from dataclasses import dataclass
from traceback import FrameSummary
from typing import Callable, Any, Dict, Union, List

import rx
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable, Disposable

import rxbp
from rxbp.flowable import Flowable
from rxbp.flowables.bufferflowable import BufferFlowable
from rxbp.flowables.mapflowable import MapFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.init.initflowable import init_flowable
from rxbp.init.initsubscription import init_subscription
from rxbp.mixins.flowablemixin import FlowableMixin
from rxbp.multicast.flowabledict import FlowableDict
from rxbp.multicast.mixins.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.mixins.multicastmixin import MultiCastMixin
from rxbp.multicast.multicasts.mapmulticast import MapMultiCast
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.multicast.multicastsubscription import MultiCastSubscription
from rxbp.multicast.typing import MultiCastItem
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


@dataclass
class LoopFlowableMultiCast(MultiCastMixin):
    """

    """

    source: MultiCastMixin
    func: Callable[[MultiCastMixin], MultiCastMixin]
    initial: Union[List[Any], Dict[Any, Any]]
    stack: List[FrameSummary]

    def unsafe_subscribe(self, subscriber: MultiCastSubscriber) -> MultiCastSubscription:
        initial = self.initial

        class StartWithInitialValueFlowable(FlowableMixin):
            def __init__(self):
                # mutable state, that will be set as soon as the first Flowable is subscribed
                self.flowable = None

            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
                return self.flowable.unsafe_subscribe(subscriber)

        start = StartWithInitialValueFlowable()
        shared = RefCountFlowable(source=start, stack=self.stack)

        # mutual curr_index variable is used to index the input Flowables
        # as the sequal to the deferred Flowables
        # {0: deferred_flowable_1,
        #  1: input_flowable_1,
        #  2: input_flowable_2,}
        curr_index = 0

        # map initial value(s) to a dictionary
        if isinstance(initial, list):
            curr_index = len(initial)
            initial_dict = {idx: val for idx, val in enumerate(initial)}

        elif isinstance(initial, dict):
            initial_dict = initial

        else:
            raise Exception(f'illegal base "{initial}"')

        def gen_deferred():
            for key in initial_dict.keys():
                yield key, init_flowable(
                    underlying=MapFlowable(
                        source=shared,
                        func=lambda d, key=key: d[key],
                    ),
                )

        deferred = dict(gen_deferred())

        def map_to_flowable_dict(base: MultiCastMixin):
            if isinstance(base, Flowable):
                states = {curr_index: base}

            elif isinstance(base, list):
                assert all(isinstance(e, Flowable) for e in base)

                states = {curr_index + idx: val for idx, val in enumerate(base)}

            elif isinstance(base, dict):
                assert all(isinstance(e, Flowable) for e in base.values())
                states = base

            elif isinstance(base, FlowableStateMixin):
                states = base.get_flowable_state()

            else:
                raise Exception(f'illegal base "{base}"')

            return FlowableDict({**states, **deferred})

        def map_func(base: MultiCastItem):
            match_error_message = f'loop_flowables function returned "{base}" which does not match initial "{initial}"'

            if isinstance(base, Flowable) and len(initial_dict) == 1:

                # if initial would be a dicitonary, then the input to the loop_flowables operator
                # must be a dicitonary and not a Flowable.
                assert not isinstance(initial, dict), match_error_message

                # create standard form
                flowable_state = {0: base}

                # function that will map the resulting state back to a Flowable
                def from_state(state):
                    return state[0]

            elif isinstance(base, list):
                assert isinstance(initial, list) and len(initial) == len(base)

                # create standard form
                flowable_state = {idx: val for idx, val in enumerate(base)}

                # # function that will map the resulting state to a list
                def from_state(state):
                    return list(state.values())

            elif isinstance(base, dict) or isinstance(base, FlowableStateMixin):
                if isinstance(base, FlowableStateMixin):
                    flowable_state = base.get_flowable_state()
                else:
                    flowable_state = base

                match_error_message = f'loop_flowables function returned "{flowable_state.keys()}", ' \
                                      f'which does not match initial "{initial.keys()}"'

                assert isinstance(initial, dict) and set(initial.keys()) <= set(flowable_state.keys()), match_error_message

                # function that will map the resulting state to a FlowableDict
                def from_state(state):
                    return FlowableDict(state)

            else:
                raise Exception(f'illegal case "{base}"')

            # share flowables
            shared_flowable_state = {key: RefCountFlowable(value, stack=self.stack) for key, value in flowable_state.items() if key in initial_dict}
            not_deferred_flowables = {key: value for key, value in flowable_state.items() if
                                     key not in initial_dict}

            lock = threading.RLock()
            is_first = [True]

            class DeferFlowable(FlowableMixin):
                def __init__(self, source: FlowableMixin, key: Any):
                    self.source = source
                    self.key = key

                def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:

                    close_loop = False
                    with lock:
                        if is_first[0]:
                            is_first[0] = False
                            close_loop = True

                    # close loop_flowables loop only if first subscribed
                    if close_loop:

                        def gen_index_for_each_deferred_state():
                            """ for each value returned by the loop_flowables function """
                            for key in initial_dict.keys():
                                yield init_flowable(MapFlowable(
                                    source=shared_flowable_state[key],
                                    func=lambda v, key=key: (key, v)),
                                )
                        indexed_deferred_values = gen_index_for_each_deferred_state()

                        zipped = rxbp.zip(*indexed_deferred_values).pipe(
                            rxbp.op.map(lambda v: dict(v)),
                        )

                        buffered = BufferFlowable(source=zipped, buffer_size=1)

                        class BreakingTheLoopFlowable(FlowableMixin):
                            def __init__(self):
                                self.disposable = SingleAssignmentDisposable()

                            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:

                                class BreakingTheLoopObservable(Observable):
                                    def observe(_, observer_info: ObserverInfo) -> rx.typing.Disposable:
                                        """
                                        this method should only be called once
                                        stop calling another observe method here
                                        """

                                        subscription = buffered.unsafe_subscribe(subscriber)
                                        disposable = subscription.observable.observe(
                                            observer_info=observer_info, #.copy(is_volatile=True),
                                        )

                                        self.disposable.set_disposable(disposable)

                                        def action(_, __):
                                            try:
                                                observer_info.observer.on_next([initial])
                                            except Exception as exc:
                                                observer_info.observer.on_error(exc)

                                        d2 = subscriber.subscribe_scheduler.schedule(action)

                                        return CompositeDisposable(self.disposable, d2)

                                return init_subscription(
                                    observable=BreakingTheLoopObservable(),
                                )

                        start.flowable = BreakingTheLoopFlowable()

                    # subscribe method call might close the loop_flowables loop
                    return self.source.unsafe_subscribe(subscriber)

            # create a flowable for all deferred values
            new_states = {
                k: init_flowable(DeferFlowable(v, k)) for k, v in shared_flowable_state.items()
            }

            return from_state({**new_states, **not_deferred_flowables})

        init = MapMultiCast(
            self.source,
            func=map_to_flowable_dict,
        )

        output = self.func(init)

        return MapMultiCast(
            source=output,
            func=map_func,
        ).unsafe_subscribe(subscriber=subscriber)
