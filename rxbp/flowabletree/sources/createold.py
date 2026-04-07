from dataclasses import dataclass
from typing import Callable, override


from lambdatrace import StackTrace

import contcert
from contcert.typing import ContCert, Scheduler, Trampoline

import rxsingle
from rxsingle.typing import (
    RxSingle,
    RxSingleNode,
    SubscribeArgs as RxSingleSubscribeArgs,
    Observer as RxSingleObserver,
)

from rxbp.cancellable import init_cancellation_state
from rxbp.flowabletree.state import State
from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.subscribeargs import SubscribeArgs
from rxbp.flowabletree.subscriptionresult import SubscriptionResult, init_subscription_result
from rxbp.flowabletree.flowablenode import FlowableNode



# @dataclass
# class CreateObserver[U](Observer[U]):
#     downstream: Observer[U] | None
#
#     @override
#     def on_next(
#         self, 
#         trampoline: Trampoline,
#         item: U, 
#         observer: RxSingleObserver[None],
#     ) -> ContCert:
#         if self.downstream is None:
#             raise Exception("Too early")
#         else:
#             return self.downstream.on_next(trampoline, item, observer)
#
#     @override
#     def on_next_and_complete(self, item: U) -> ContCert:
#         if self.downstream is None:
#             raise Exception("Too early")
#         else:
#             return self.downstream.on_next_and_complete(item)
#
#     @override
#     def on_completed(self) -> ContCert:
#         if self.downstream is None:
#             raise Exception("Too early")
#         else:
#             return self.downstream.on_completed()
#
#     @override
#     def on_error(self, exception: Exception) -> ContCert:
#         if self.downstream is None:
#             raise Exception("Too early")
#         else:
#             return self.downstream.on_error(exception)

@dataclass
class Notification[A, U]: ...

@dataclass
class Next[A, U](Notification[A, U]):
    acc: A
    value: U

@dataclass
class NextAndComplete[A, U](Notification[A, U]):
    value: U

@dataclass
class Complete[A, U](Notification[A, U]): ...


@dataclass
class CreateObserver[U](RxSingleObserver[U]):
    @override
    def on_success(
        self,
        trampoline: Trampoline,
        item: U,
    ) -> ContCert:
        ...

    @override
    def on_error(
        self,
        trampoline: Trampoline,
        exception: Exception,
    ) -> ContCert:
        ...



@dataclass
class OnNextRxSingeNode[S: State, U](RxSingleNode[S, None]):
    observer: Observer[U]
    item: U

    @override
    def subscribe(
            self,
            state: S, 
            args: RxSingleSubscribeArgs[None],
    ) -> tuple[S, ContCert]:
        cert = self.observer.on_next(
            trampoline=state.p_subscription_trampoline,
            item=self.item,
            observer=args.observer,
        )
        return state, cert


@dataclass
class StopObserver[U](RxSingleObserver[ContCert]):
    # on_error_func: Callable[[Exception], ContCert]
    observer: Observer[U]

    @override
    def on_success(self, trampoline: Trampoline, item: ContCert):
        return item

    @override
    def on_error(
        self, trampoline: Trampoline, exception: Exception
    ) -> ContCert:
        # return self.on_error_func(exception)
        return self.observer.on_error(exception)


@dataclass(frozen=True)
class Create[S: State, A, U](FlowableNode[S, U]):
    # func: Callable[[Observer[U]], RxSingle[State, ContCert]]
    # observer: CreateObserver[U]
    init: RxSingleNode[S, A]
    acc_func: Callable[[A, Scheduler], RxSingleNode[S, Notification[A, U]]]
    # source: RxSingleNode[S, ContCert]
    stacks: tuple[StackTrace, ...]

    # @override
    # def discover(self, state: S) -> S:
    #     state = self.source.discover(state)
    #     return state

    @override
    def unsafe_subscribe(
        self,
        state: S,
        args: SubscribeArgs[U],
    ) -> tuple[S, SubscriptionResult]:
        cancellable = init_cancellation_state()

        def next_iteration(acc: A) -> RxSingleNode[S, ContCert]:
            def handle_notification(notification: Notification[A, U]) -> RxSingleNode[S, ContCert]:
                match notification:
                    case Next(acc=acc, value=value):
                        def next_iteration_acc(_: None, acc: A = acc) -> RxSingleNode[S, ContCert]:
                            return next_iteration(acc=acc)

                        # cert = args.observer.on_next(trampoline, value, observer)
                        node = OnNextRxSingeNode[S, U](observer=args.observer, item=value)
                        next = rxsingle.op.flat_map(
                            child=node, 
                            func=next_iteration_acc, 
                            stacks=tuple(),
                        )
                        return next

                    case NextAndComplete(value=value):
                        cert = args.observer.on_next_and_complete(value)
                        return rxsingle.from_(cert)

                    case Complete():
                        cert = args.observer.on_completed()
                        return rxsingle.from_(cert)

                    case _:
                        raise Exception(f'Unexpected notification {notification}')

            next = self.acc_func(acc, args.scheduler)
            return rxsingle.op.flat_map(next, handle_notification, stacks=tuple())

        state, cert = rxsingle.op.flat_map(self.init, next_iteration, stacks=tuple()).subscribe(
            state=state,
            args=rxsingle.init_subscribe_args(
                observer=StopObserver(
                    observer=args.observer,
                ),
                virtual_count=args.virtual_count,
                context=args.context,
            )
        )

        result = init_subscription_result(
            cancellable=cancellable,
            certificate=cert,
        )

        return state, result

        # observer = CreateObserver(
        #     downstream=args.observer,
        #     is_completed=False,
        # )

        # def ensure_completion(certificate):
        #     if isinstance(certificate, ContCert):
        #         return rxsingle.from_value(certificate)
        #
        #     else:
        #         return args.observer.on_completed()

        # source = self.func(args.observer)
        # source = (
        #     rxsingle.from_value[State, None](None)
        #     .flat_map(lambda _: self.func(args.observer, args.scheduler))
        #     # .flat_map(ensure_completion)
        # )

        # def assign_observer():
        #     self.observer.downstream = args.observer

        # task = contcert.init_task(
        #     callable=assign_observer,
        # )

        # state.subscribe_trampoline.schedule(
        #     task=,
        #     stacks=self.stacks,
        # )

        # certificate = rxsingle.fork(
        #     state=state,
        #     source=self.source,
        #     on_error=args.observer.on_error,
        #     scheduler=state.subscription_trampoline,  # ensures scheduling on trampoline
        #     cancellation=cancellable,
        #     virtual_count=args.virtual_count,
        # )

        return state, SubscriptionResult(
            certificate=certificate,
            cancellable=init_cancellation_state(),
        )


def init_create[S: State, A, U](
    init: RxSingleNode[S, A],
    acc_func: Callable[[A], RxSingleNode[S, Notification[A, U]]],
    # func: Callable[[Observer[U]], RxSingleNode[S, ContCert]],
):

    # observer = CreateObserver[U](downstream=None)
    # source = func(observer)
    return Create[S, A, U](
        init=init,
        acc_func=acc_func,
        stacks=tuple(),
    )
