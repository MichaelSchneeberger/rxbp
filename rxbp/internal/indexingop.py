from rxbp.internal.indexing import OnCompleted, OnNext
from rxbp.observable import Observable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.matchobservable import match
from rxbp.testing.debugobservable import DebugObservable


def merge_indexes(left: Observable, right: Observable):
    obs = match(left, right,
                request_left=lambda l, r: isinstance(l, OnCompleted) or isinstance(r, OnCompleted),
                request_right=lambda l, r: isinstance(l, OnNext),
                match_func=lambda l, r: (isinstance(l, OnNext) and isinstance(r, OnNext)) or isinstance(l, OnCompleted))
    result = MapObservable(obs, lambda t2: t2[0])
    return result


def index_observable(obs: Observable, selector: Observable):
    obs = match(obs, selector,
                request_left=lambda l, r: isinstance(r, OnCompleted),
                request_right=lambda l, r: True,
                match_func=lambda l, r: isinstance(r, OnNext))
    result = MapObservable(obs, lambda t2: t2[0])
    return result

