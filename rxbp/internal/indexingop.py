from rxbp.internal.indexing import OnCompleted, OnNext, on_completed_idx, on_next_idx
from rxbp.observable import Observable
from rxbp.observables.concatobservable import ConcatObservable
from rxbp.observables.connectableobservable import ConnectableObservable
from rxbp.observables.filterobservable import FilterObservable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.matchobservable import match
from rxbp.observables.mergeobservable import merge
from rxbp.observables.nowobservable import NowObservable
from rxbp.scheduler import Scheduler
from rxbp.subjects.publishsubject import PublishSubject
from rxbp.testing.debugobservable import DebugObservable


def merge_indexes(left: Observable, right: Observable, scheduler: Scheduler, subscribe_scheduler: Scheduler):
    # right_ = ConcatObservable([NowObservable(on_completed_idx, subscribe_scheduler=subscribe_scheduler), right])

    # mem = [True]
    #
    # def request_right(l, r):
    #     if isinstance(l, OnNext):
    #         if isinstance(r, OnCompleted):
    #             print('request right')
    #             return_val = mem[0]
    #             mem[0] = not return_val
    #             print(return_val)
    #             return return_val
    #         else:
    #             return True
    #     else:
    #         return False

    # obs = match(DebugObservable(left, 'left'), DebugObservable(right_, 'right'),
    #             request_left=lambda l, r: isinstance(l, OnCompleted) or isinstance(r, OnCompleted),
    #             request_right=lambda l, r: isinstance(l, OnNext) or isinstance(r, OnCompleted),
    #             match_func=lambda l, r: r is not None)
    # result = DebugObservable(MapObservable(obs, lambda t2: t2[0]), 'out')

    obs = match(DebugObservable(FilterObservable(left, lambda v: isinstance(v, OnNext), scheduler=scheduler)), DebugObservable(right),
                request_left=lambda l, r: isinstance(r, OnCompleted),
                request_right=lambda l, r: True,
                match_func=lambda l, r: isinstance(r, OnNext))
    result = DebugObservable(MapObservable(obs, lambda t2: t2[0]))

    o1 = FilterObservable(left, lambda v: isinstance(v, OnCompleted), scheduler=scheduler)
    o2 = merge(o1, result)
    o3 = ConnectableObservable(source=o2, scheduler=scheduler, subscribe_scheduler=subscribe_scheduler).ref_count()

    return o3


def index_observable(obs: Observable, selector: Observable):
    obs = match(obs, selector,
                request_left=lambda l, r: isinstance(r, OnCompleted),
                request_right=lambda l, r: True,
                match_func=lambda l, r: isinstance(r, OnNext))
    result = MapObservable(obs, lambda t2: t2[0])
    return result

