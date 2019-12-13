import threading
from typing import Callable, Any, Dict, Union, List

import rx
import rxbp
from rx import operators as rxop
from rx.core.typing import Disposable
from rx.disposable import SingleAssignmentDisposable, CompositeDisposable
from rxbp.flowable import Flowable
from rxbp.flowablebase import FlowableBase
from rxbp.flowables.bufferflowable import BufferFlowable
from rxbp.flowables.mapflowable import MapFlowable
from rxbp.flowables.refcountflowable import RefCountFlowable
from rxbp.multicast.flowabledict import FlowableDict
from rxbp.multicast.flowablestatemixin import FlowableStateMixin
from rxbp.multicast.multicastInfo import MultiCastInfo
from rxbp.multicast.multicastbase import MultiCastBase
from rxbp.multicast.multicasts.mapmulticast import MapMultiCast
from rxbp.multicast.singleflowablemixin import SingleFlowableMixin
from rxbp.multicast.typing import MultiCastValue
from rxbp.observable import Observable
from rxbp.observerinfo import ObserverInfo
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo


class DeferMultiCast(MultiCastBase):
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

        curr_index = 0
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
                    return key, Flowable(MapFlowable(source=shared, selector=lambda d: d[key]))

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

                if isinstance(base, SingleFlowableMixin):
                    states = {**states, curr_index: base.get_single_flowable()}

            else:
                raise Exception(f'illegal base "{base}"')

            return FlowableDict({**states, **deferred})

        init = MapMultiCast(self.source, func=map_to_flowable_dict)
        output = self.func(init)

        def map_func(base: MultiCastValue):

            def select_first_index(state):
                class SingleFlowableDict(SingleFlowableMixin, FlowableDict):
                    def get_single_flowable(self) -> Flowable:
                        return state[0]

                return SingleFlowableDict(state)

            def select_none(state):
                return FlowableDict(state)

            match_error_message = f'defer function returned "{base}" which does not match initial "{initial}"'

            if isinstance(base, Flowable) and len(initial_dict) == 1:
                assert not isinstance(initial, dict), match_error_message

                if isinstance(initial, list):
                    assert len(base) == 1, match_error_message

                deferred_values = {list(initial_dict.keys())[0]: base}      # deferred values refer to the values returned by the defer function
                select_flowable_dict = select_none

            elif isinstance(base, list):
                assert isinstance(initial, list) and len(initial) == len(base)

                deferred_values = {idx: val for idx, val in enumerate(base)}
                select_flowable_dict = select_first_index

            elif isinstance(base, dict) or isinstance(base, FlowableStateMixin):
                if isinstance(base, FlowableStateMixin):
                    deferred_values = base.get_flowable_state()
                else:
                    deferred_values = base

                match_error_message = f'defer function returned "{deferred_values.keys()}", ' \
                                      f'which does not match initial "{initial.keys()}"'

                assert isinstance(initial, dict) and set(initial.keys()) <= set(deferred_values.keys()), match_error_message

                select_flowable_dict = select_none

            else:
                raise Exception(f'illegal case "{base}"')

            shared_deferred_values = {key: RefCountFlowable(value) for key, value in deferred_values.items()}

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

                    # close defer loop only if first element has received
                    if close_loop:

                        def gen_index_for_each_deferred_state():
                            """ for each value returned by the defer function """
                            for key in initial_dict.keys():
                                def for_func(key=key):
                                    return Flowable(MapFlowable(shared_deferred_values[key], selector=lambda v: (key, v)))
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
                                    info=SubscriptionInfo(None),
                                    observable=BreakingTheLoopObservable(),
                                )

                        start.flowable = BreakingTheLoopFlowable()

                    # subscribe method call might close the defer loop
                    subscription = self.source.unsafe_subscribe(subscriber)

                    class DeferObservable(Observable):
                        def observe(self, observer_info: ObserverInfo):
                            disposable = subscription.observable.observe(observer_info)

                            return disposable

                    defer_observable = DeferObservable()

                    return Subscription(info=SubscriptionInfo(None), observable=defer_observable)

            # create a flowable for all deferred values
            new_states = {k: Flowable(DeferFlowable(v, k)) for k, v in shared_deferred_values.items()}

            return select_flowable_dict(new_states)

        return output.get_source(info=info).pipe(
            rxop.map(map_func),
        )