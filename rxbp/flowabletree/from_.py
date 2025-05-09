import datetime
from typing import Iterable

from donotation import do

import continuationmonad
from continuationmonad.typing import Scheduler

from rxbp.flowabletree.observer import Observer
from rxbp.flowabletree.sources.create import init_create



def count():
    @do()
    def count_and_send_item(observer: Observer, count: int):
        yield observer.on_next(count)

        yield from continuationmonad.schedule_trampoline()

        return count_and_send_item(observer, count + 1)

    return init_create(
        func=lambda observer, _: count_and_send_item(observer, 0),
    )


def empty():
    return init_create(
        func=lambda observer, _: observer.on_completed(),
    )


def error(exception: Exception):
    return init_create(
        func=lambda observer, _: observer.on_error(exception),
    )


def from_iterable[U](iterable: Iterable[U]):
    @do()
    def schedule_and_send_next(current_item: U, observer: Observer, iterator):
        try:
            next_item, has_item = next(iterator), True

        except StopIteration:
            next_item, has_item = None, False

        if has_item:
            _ = yield from observer.on_next(current_item)

            yield from continuationmonad.schedule_trampoline()

            return schedule_and_send_next(next_item, observer, iterator)

        else:
            return observer.on_next_and_complete(current_item)

    def start_procedure(observer: Observer, _):
        iterator = iter(iterable)

        try:
            next_item = next(iterator)
            return schedule_and_send_next(next_item, observer, iterator)

        except StopIteration:
            return observer.on_completed()

    return init_create(func=start_procedure)


def from_value(value):
    return init_create(
        func=lambda observer, _: observer.on_next_and_complete(value),
    )


def interval(seconds: float, scheduler: Scheduler | None = None):
    @do()
    def schedule_and_send_item(
        observer: Observer,
        scheduler: Scheduler,
        prev_duetime: datetime.datetime,
    ):
        duetime = prev_duetime + datetime.timedelta(seconds=seconds)
        yield continuationmonad.schedule_absolute(duetime, scheduler)
        yield observer.on_next(duetime)
        return schedule_and_send_item(observer, scheduler, duetime)

    def start_procedure(observer: Observer, _scheduler: Scheduler):
        if scheduler is None:
            sel_scheduler = _scheduler
        else:
            sel_scheduler = scheduler

        start_time = sel_scheduler.now()
        return schedule_and_send_item(observer, sel_scheduler, start_time)

    return init_create(
        func=start_procedure,
    )


def repeat_value[U](value: U):
    @do()
    def schedule_and_send_item(observer: Observer[U], scheduler):
        yield observer.on_next(value)

        yield from continuationmonad.schedule_trampoline()

        return schedule_and_send_item(observer, scheduler)

    return init_create(
        func=schedule_and_send_item,
    )


def schedule_on(scheduler: Scheduler | None = None):
    @do()
    def schedule_and_send_item(observer: Observer, _scheduler: Scheduler):
        if scheduler is None:
            sel_scheduler = _scheduler
        else:
            sel_scheduler = scheduler

        yield continuationmonad.schedule_on(sel_scheduler)
        return observer.on_next_and_complete(sel_scheduler)

    return init_create(
        func=schedule_and_send_item,
    )


def schedule_relative(duetime: float, scheduler: Scheduler | None = None):
    @do()
    def schedule_and_send_item(observer: Observer, _scheduler: Scheduler):
        if scheduler is None:
            sel_scheduler = _scheduler
        else:
            sel_scheduler = scheduler

        yield continuationmonad.schedule_relative(duetime, sel_scheduler)
        return observer.on_next_and_complete(scheduler)

    return init_create(
        func=schedule_and_send_item,
    )


def schedule_absolute(duetime: datetime.datetime, scheduler: Scheduler | None = None):
    @do()
    def schedule_and_send_item(observer: Observer, _scheduler: Scheduler):
        if scheduler is None:
            sel_scheduler = _scheduler
        else:
            sel_scheduler = scheduler

        yield continuationmonad.schedule_absolute(duetime, sel_scheduler)
        return observer.on_next_and_complete(scheduler)

    return init_create(
        func=schedule_and_send_item,
    )
