from rxbp.internal.indexing import OnCompleted, OnNext, on_completed_idx, on_next_idx
from rxbp.observable import Observable
from rxbp.observables.connectableobservable import ConnectableObservable
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.observables.filterobservable import FilterObservable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.mergeobservable import merge
from rxbp.scheduler import Scheduler
from rxbp.testing.debugobservable import DebugObservable


def merge_indexes(left: Observable, right: Observable, scheduler: Scheduler, subscribe_scheduler: Scheduler):
    obs = ControlledZipObservable(DebugObservable(FilterObservable(left, lambda v: isinstance(v, OnNext), scheduler=scheduler)), DebugObservable(right),
                request_left=lambda l, r: isinstance(r, OnCompleted),
                request_right=lambda l, r: True,
                match_func=lambda l, r: isinstance(r, OnNext),
                                  scheduler=scheduler)
    result = DebugObservable(MapObservable(obs, lambda t2: t2[0]))

    o1 = FilterObservable(left, lambda v: isinstance(v, OnCompleted), scheduler=scheduler)
    o2 = merge(o1, result)
    o3 = ConnectableObservable(source=o2, scheduler=scheduler, subscribe_scheduler=subscribe_scheduler).ref_count()

    return o3


def index_observable(obs: Observable, selector: Observable, scheduler: Scheduler):
    obs = ControlledZipObservable(obs, selector,
                request_left=lambda l, r: isinstance(r, OnCompleted),
                request_right=lambda l, r: True,
                match_func=lambda l, r: isinstance(r, OnNext),
                                  scheduler=scheduler)
    result = MapObservable(obs, lambda t2: t2[0])
    return result

