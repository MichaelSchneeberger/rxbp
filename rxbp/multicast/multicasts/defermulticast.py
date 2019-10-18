import threading
from typing import Callable, Any, Dict, Union, List

import rx
from rx import operators as rxop

import rxbp
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
            else:
                raise Exception(f'illegal base "{base}"')

            return FlowableDict({**states, **deferred})

        init = MapMultiCast(self.source, func=map_to_flowable_dict)
        output = self.func(init)

        def map_func(base: MultiCastValue):
            # the first state that is subscribed initializes the defer loop
            if isinstance(base, Flowable) and len(initial_dict) == 1:
                states = {list(initial_dict.keys())[0]: base}
            elif isinstance(base, list):
                states = {idx: val for idx, val in enumerate(base)}
            elif isinstance(base, dict):
                states = base
            elif isinstance(base, FlowableStateMixin):
                states: Dict[Any, Flowable] = base.get_flowable_state()
            else:
                raise Exception(f'illegal case "{base}"')

            lock = threading.RLock()
            is_first = [True]
            # is_subscribed = [False]
            # is_connected = [False]
            # breaking_the_loop = [None]

            class DeferFlowable(FlowableBase):
                def __init__(self, source: FlowableBase, key: Any):
                    self.source = source
                    self.key = key

                def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
                    # print('DeferFlowable.subscribe')

                    create_loop = False
                    with lock:
                        if is_first[0]:
                            is_first[0] = False
                            create_loop = True

                    if create_loop:
                        def gen_flowable_per_deferred_state():
                            for key in initial_dict.keys():
                                def for_func(key=key):
                                    return states[key].map(lambda v: (key, v))
                                yield for_func()

                        zipped = rxbp.zip(*gen_flowable_per_deferred_state()).pipe(
                            rxbp.op.map(lambda v: dict(v)),
                        )

                        buffered = BufferFlowable(source=zipped, buffer_size=1)

                        class BreakingTheLoopFlowable(FlowableBase):
                            def __init__(self):
                                self.observer = None
                                self.disposable = SingleAssignmentDisposable()

                            def connect(self):
                                subscription = buffered.unsafe_subscribe(subscriber)
                                volatile = ObserverInfo(observer=self.observer, is_volatile=True)
                                disposable = subscription.observable.observe(volatile)
                                self.disposable.set_disposable(disposable)

                            def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
                                class BreakingTheLoopObservable(Observable):
                                    def observe(_, observer_info: ObserverInfo) -> Disposable:
                                        self.observer = observer_info.observer
                                        self.connect()

                                        def action(_, __):
                                            self.observer.on_next([initial])

                                        d2 = subscriber.subscribe_scheduler.schedule(action)

                                        # return self.disposable

                                        return CompositeDisposable(self.disposable, d2)

                                return Subscription(
                                    info=SubscriptionInfo(None),
                                    observable=BreakingTheLoopObservable(),
                                )

                        start.flowable = BreakingTheLoopFlowable()

                    subscription = states[self.key].unsafe_subscribe(subscriber)

                    class DeferObservable(Observable):
                        def observe(self, observer_info: ObserverInfo):
                            disposable = subscription.observable.observe(observer_info)

                            # connect = False
                            # with lock:
                            #     if not is_connected[0]:
                            #         is_connected[0] = True
                            #         connect = True
                            #
                            # if connect:
                            #     breaking_the_loop[0].connect()

                            return disposable

                    defer_observable = DeferObservable()

                    return Subscription(info=SubscriptionInfo(None), observable=defer_observable)

            # do this over all states
            new_states = {k: Flowable(DeferFlowable(v, k)) for k, v in states.items()}

            return FlowableDict(new_states)

        return output.get_source(info=info).pipe(
            rxop.map(map_func),
        )