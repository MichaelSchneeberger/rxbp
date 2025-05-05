import datetime
from typing import Iterable

from continuationmonad.typing import Scheduler

from rxbp.flowable.init import init_connectable_flowable, init_flowable
from rxbp.flowabletree.sources.fromrx import FromRx
from rxbp.flowabletree.nodes import FlowableNode
from rxbp.flowabletree.operations.buffer.flowable import init_buffer
from rxbp.flowabletree.sources.connectable import init_connectable
from rxbp.flowabletree.operations.merge.flowable import init_merge
from rxbp.flowabletree.operations.zip.flowable import init_zip
from rxbp.flowabletree.from_ import (
    count as _count,
    empty as _empty,
    error as _error,
    from_iterable as _from_iterable,
    from_value as _from_value,
    interval as _interval,
    repeat_value as _repeat,
    schedule_on as _schedule_on,
    schedule_relative as _schedule_relative,
    schedule_absolute as _schedule_absolute,
)


def connectable(id, init):
    return init_connectable_flowable(child=init_connectable(id, init))


def count():
    return init_flowable(_count())


def empty():
    return init_flowable(_empty())


def error(exception: Exception):
    return init_flowable(_error(exception))


def from_iterable[U](iterable: Iterable[U]):
    return init_flowable(_from_iterable(iterable))


def from_value(value):
    return init_flowable(_from_value(value))


def from_rx(source):
    return init_flowable(
        child=init_buffer(
            child=FromRx(source=source),
        ),
    )


def interval(scheduler: Scheduler, seconds: float):
    return init_flowable(_interval(scheduler, seconds))


def merge(observables: tuple[FlowableNode, ...]):
    return init_flowable(
        child=init_merge(children=observables),
    )


def repeat(value):
    return init_flowable(
        child=_repeat(value=value),
    )


def schedule_on(scheduler: Scheduler):
    return init_flowable(_schedule_on(scheduler))


def schedule_relative(scheduler: Scheduler, duetime: float):
    return init_flowable(_schedule_relative(scheduler, duetime))


def schedule_absolute(scheduler: Scheduler, duetime: datetime.datetime):
    return init_flowable(_schedule_absolute(scheduler, duetime))


def zip(observables: tuple[FlowableNode, ...]):
    return init_flowable(
        child=init_zip(children=observables),
    )
