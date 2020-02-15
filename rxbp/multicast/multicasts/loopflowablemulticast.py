import threading
from typing import Callable, Any, Dict, Union, List

import rx
from rx import operators as rxop
from rx.core.typing import Disposable
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable

import rxbp
from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowables.bufferflowable import BufferFlowable
from rxbp.flowables.mapflowable import MapFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.multicast.flowabledict import FlowableDict
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicastflowable import MultiCastFlowable
from rxbp.multicast.multicasts.mapmulticast import MapMultiCast
from rxbp.multicast.typing import MultiCastValue
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.selectors.baseandselectors import BaseAndSelectors
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription


class LoopFlowableMultiCast(MultiCastBase):
    def __init__(
            self,
            source: MultiCastBase,
            func: Callable[[MultiCastBase], MultiCastBase],
            initial: Union[List[Any], Dict[Any, Any]],
    ):
        self.source = source
        self.func = func
        self.initial = initial

    def get_source(self, info: MultiCastInfo) -> rx.typing.Observable:
        initial = self.initial

        class StartWithInitialValueFlowable(FlowableBase):
            def __init__(self):
                # mutable state, that will be set as soon as the first Flowable is subscribed
                self.flowable = None

            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
                subscription = self.flowable.unsafe_subscribe(subscriber)

                return Subscription(
                    info=subscription.info,
                    observable=subscription.observable,
                )

        start = StartWithInitialValueFlowable()
        shared = RefCountFlowable(source=start)

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
                def for_func(key=key):
                    return key, MultiCastFlowable(MapFlowable(source=shared, func=lambda d: d[key]))

                yield for_func()

        deferred = dict(gen_deferred())

        def map_to_flowable_dict(base: MultiCastBase):
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

                # if isinstance(base, SingleFlowableMixin):
                #     states = {**states, curr_index: base.get_single_flowable()}

            else:
                raise Exception(f'illegal base "{base}"')

            return FlowableDict({**states, **deferred})

        init = MapMultiCast(self.source, func=map_to_flowable_dict)
        output = self.func(init)

        def map_func(base: MultiCastValue):
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
            shared_flowable_state = {key: RefCountFlowable(value) for key, value in flowable_state.items() if key in initial_dict}
            not_deferred_flowables = {key: value for key, value in flowable_state.items() if
                                     key not in initial_dict}

            lock = threading.RLock()
            is_first = [True]

            class DeferFlowable(FlowableBase):
                def __init__(self, source: FlowableBase, key: Any):
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
                                def for_func(key=key):
                                    return Flowable(MapFlowable(shared_flowable_state[key], func=lambda v: (key, v)))
                                yield for_func()
                        indexed_deferred_values = gen_index_for_each_deferred_state()

                        zipped = rxbp.zip(*indexed_deferred_values).pipe(
                            rxbp.op.map(lambda v: dict(v)),
                        )

                        buffered = BufferFlowable(source=zipped, buffer_size=1)

                        class BreakingTheLoopFlowable(FlowableBase):
                            def __init__(self):
                                self.disposable = SingleAssignmentDisposable()

                            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:

                                class BreakingTheLoopObservable(Observable):
                                    def observe(_, observer_info: ObserverInfo) -> Disposable:
                                        """
                                        this method should only be called once
                                        stop calling another observe method here
                                        """

                                        observer = observer_info.observer

                                        subscription = buffered.unsafe_subscribe(subscriber)

                                        observer_info = ObserverInfo(observer=observer, is_volatile=True)
                                        disposable = subscription.observable.observe(observer_info)

                                        self.disposable.set_disposable(disposable)

                                        def action(_, __):
                                            observer.on_next([initial])

                                        d2 = subscriber.subscribe_scheduler.schedule(action)

                                        return CompositeDisposable(self.disposable, d2)

                                return Subscription(
                                    info=BaseAndSelectors(None),
                                    observable=BreakingTheLoopObservable(),
                                )

                        start.flowable = BreakingTheLoopFlowable()

                    # subscribe method call might close the loop_flowables loop
                    subscription = self.source.unsafe_subscribe(subscriber)

                    class DeferObservable(Observable):
                        def observe(self, observer_info: ObserverInfo):
                            disposable = subscription.observable.observe(observer_info)

                            return disposable

                    defer_observable = DeferObservable()

                    return Subscription(info=BaseAndSelectors(None), observable=defer_observable)

            # create a flowable for all deferred values
            new_states = {
                k: MultiCastFlowable(DeferFlowable(v, k)) for k, v in shared_flowable_state.items()
            }

            return from_state({**new_states, **not_deferred_flowables})

        return output.get_source(info=info).pipe(
            rxop.map(map_func),
        )