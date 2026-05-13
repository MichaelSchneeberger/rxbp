from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import Never, override

from contcert.typing import Trampoline, LoopLikeScheduler
from rxsingle.typing import Observer as RxSingleObserver



@dataclass
class OnNext[U](ABC):
    @property
    @abstractmethod
    def p_item(self) -> U: ...

    @property
    @abstractmethod
    def p_trampoline(self) -> Trampoline: ...

    @property
    @abstractmethod
    def p_item_scheduler(self) -> LoopLikeScheduler: ...

    @property
    @abstractmethod
    def p_observer(self) -> RxSingleObserver[None, Never]: ...

    @abstractmethod
    def update_item[V](self, item: V) -> OnNext[V]: ...


@dataclass
class OnNextAndComplete[U](ABC):
    @property
    @abstractmethod
    def p_item(self) -> U: ...

    @property
    @abstractmethod
    def p_trampoline(self) -> Trampoline: ...

    @property
    @abstractmethod
    def p_item_scheduler(self) -> LoopLikeScheduler: ...

    @abstractmethod
    def update_item[V](self, item: V) -> OnNextAndComplete[V]: ...



@dataclass
class OnCompleted(ABC):
    @property
    @abstractmethod
    def p_trampoline(self) -> Trampoline: ...

    @property
    @abstractmethod
    def p_item_scheduler(self) -> LoopLikeScheduler: ...



@dataclass
class OnError[E](ABC):
    @property
    @abstractmethod
    def p_exception(self) -> E: ...

    @property
    @abstractmethod
    def p_trampoline(self) -> Trampoline: ...

    @property
    @abstractmethod
    def p_item_scheduler(self) -> LoopLikeScheduler: ...


type Notification[U, E] = (
    OnNext[U] | OnNextAndComplete[U] | OnCompleted | OnError[E]
)


@dataclass
class OnNextImpl[U](OnNext[U]):
    item: U
    trampoline: Trampoline
    item_scheduler: LoopLikeScheduler
    observer: RxSingleObserver[None, Never]

    @property
    @override
    def p_item(self) -> U:
        return self.item

    @property
    @override
    def p_trampoline(self) -> Trampoline:
        return self.trampoline

    @property
    @override
    def p_item_scheduler(self) -> LoopLikeScheduler:
        return self.item_scheduler

    @property
    @override
    def p_observer(self) -> RxSingleObserver[None, Never]:
        return self.observer

    @override
    def update_item[V](self, item: V) -> OnNext[V]:
        return replace(self, item=item)  # pyright: ignore[reportReturnType]



def init_on_next[U](
    item: U,
    trampoline: Trampoline,
    item_scheduler: LoopLikeScheduler,
    observer: RxSingleObserver[None, Never],
) -> OnNext[U]:
    return OnNextImpl(
        item=item,
        trampoline=trampoline,
        item_scheduler=item_scheduler,
        observer=observer,
    )


@dataclass
class OnNextAndCompleteImpl[U](OnNextAndComplete[U]):
    item: U
    trampoline: Trampoline
    item_scheduler: LoopLikeScheduler

    @property
    @override
    def p_item(self) -> U:
        return self.item

    @property
    @override
    def p_trampoline(self) -> Trampoline:
        return self.trampoline

    @property
    @override
    def p_item_scheduler(self) -> LoopLikeScheduler:
        return self.item_scheduler

    @override
    def update_item[V](self, item: V) -> OnNextAndComplete[V]:
        return replace(self, item=item)  # pyright: ignore[reportReturnType]

def init_on_next_and_complete[U](
    item: U,
    trampoline: Trampoline,
    item_scheduler: LoopLikeScheduler,
) -> OnNextAndComplete[U]:
    return OnNextAndCompleteImpl(
        item=item,
        trampoline=trampoline,
        item_scheduler=item_scheduler,
    )


@dataclass
class OnCompletedImpl(OnCompleted):
    trampoline: Trampoline
    item_scheduler: LoopLikeScheduler

    @property
    @override
    def p_trampoline(self) -> Trampoline:
        return self.trampoline

    @property
    @override
    def p_item_scheduler(self) -> LoopLikeScheduler:
        return self.item_scheduler



def init_on_completed(
    trampoline: Trampoline,
    item_scheduler: LoopLikeScheduler,
) -> OnCompleted:
    return OnCompletedImpl(
        trampoline=trampoline,
        item_scheduler=item_scheduler,
    )


@dataclass
class OnErrorImpl[E](OnError[E]):
    exception: E
    trampoline: Trampoline
    item_scheduler: LoopLikeScheduler

    @property
    @override
    def p_exception(self) -> E:
        return self.exception

    @property
    @override
    def p_trampoline(self) -> Trampoline:
        return self.trampoline

    @property
    @override
    def p_item_scheduler(self) -> LoopLikeScheduler:
        return self.item_scheduler



def init_on_error[E](
    exception: E,
    trampoline: Trampoline,
    item_scheduler: LoopLikeScheduler,
) -> OnError[E]:
    return OnErrorImpl(
        exception=exception,
        trampoline=trampoline,
        item_scheduler=item_scheduler,
    )


# @dataclass(frozen=True)
# class OnNextArg[U]:
#     item: U
#     trampoline: Trampoline
#     scheduler: LoopLikeScheduler
#     observer: RxSingleObserver[None]
#
#
# @dataclass(frozen=True)
# class OnNextAndCompleteArg[U]:
#     item: U
#     trampoline: Trampoline
#
#
# @dataclass(frozen=True)
# class OnCompletedArg:
#     trampoline: Trampoline
#
#
# @dataclass(frozen=True)
# class OnErrorArg[E: Exception]:
#     exception: E
#     trampoline: Trampoline

