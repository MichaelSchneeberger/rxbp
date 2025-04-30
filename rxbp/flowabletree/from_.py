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
        func=lambda observer: count_and_send_item(observer, 0),
    )



def empty():
    return init_create(
        func=lambda observer: observer.on_completed(),
    )


def error(exception: Exception):
    return init_create(
        func=lambda observer: observer.on_error(exception),
    )


def from_iterable[U](iterable: Iterable[U]):
    iterator = iter(iterable)

    @do()
    def schedule_and_send_next(current_item: U, observer: Observer, iterator=iterator):
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

    def start_procedure(observer: Observer, iterator=iterator):
        try:
            next_item = next(iterator)
            return schedule_and_send_next(next_item, observer, iterator)

        except StopIteration:
            return observer.on_completed()

    return init_create(func=start_procedure)


def from_value(value):
    return init_create(
        func=lambda observer: observer.on_next_and_complete(value),
    )


def interval(scheduler: Scheduler, seconds: float):
    @do()
    def schedule_and_send_item(
        observer: Observer,
        prev_duetime: datetime.datetime,
    ):
        duetime = prev_duetime + datetime.timedelta(seconds=seconds)
        yield continuationmonad.schedule_absolute(scheduler, duetime)
        yield observer.on_next(duetime)
        return schedule_and_send_item(observer, duetime)

    def start_procedure(observer: Observer):
        start_time = scheduler.now()
        return schedule_and_send_item(observer, start_time)

    return init_create(
        func=start_procedure,
    )


def repeat_value[U](item: U):
    @do()
    def schedule_and_send_item(observer: Observer):
        yield observer.on_next(item)

        yield from continuationmonad.schedule_trampoline()

        return schedule_and_send_item(observer)

    return init_create(
        func=schedule_and_send_item,
    )


def schedule_on(scheduler: Scheduler):
    @do()
    def schedule_and_send_item(observer: Observer):
        yield continuationmonad.schedule_on(scheduler)
        return observer.on_next_and_complete(scheduler)

    return init_create(
        func=schedule_and_send_item,
    )


def schedule_relative(scheduler: Scheduler, duetime: float):
    @do()
    def schedule_and_send_item(observer: Observer):
        yield continuationmonad.schedule_relative(scheduler, duetime)
        return observer.on_next_and_complete(scheduler)

    return init_create(
        func=schedule_and_send_item,
    )


def schedule_absolute(scheduler: Scheduler, duetime: datetime.datetime):
    @do()
    def schedule_and_send_item(observer: Observer):
        yield continuationmonad.schedule_absolute(scheduler, duetime)
        return observer.on_next_and_complete(scheduler)

    return init_create(
        func=schedule_and_send_item,
    )
