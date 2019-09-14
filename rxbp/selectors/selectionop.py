from rxbp.selectors.observables.mergeselectorobservable import MergeSelectorObservable
from rxbp.selectors.selectionmsg import SelectCompleted, SelectNext
from rxbp.observable import Observable
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.scheduler import Scheduler
from rxbp.observablesubjects.publishosubject import PublishOSubject
from rxbp.testing.debugobservable import DebugObservable


def merge_selectors(left: Observable, right: Observable, scheduler: Scheduler):
    obs = MergeSelectorObservable(
        DebugObservable(left),
        DebugObservable(right),
        scheduler=scheduler,
    )
    o3 = RefCountObservable(source=obs, subject=PublishOSubject(scheduler=scheduler))

    return o3


def select_observable(obs: Observable, selector: Observable, scheduler: Scheduler):
    def request_left(left, right):
        return isinstance(right, SelectCompleted)

    obs = ControlledZipObservable(
        obs,
        selector,
        request_left=request_left,
        request_right=lambda l, r: True,
        match_func=lambda l, r: isinstance(r, SelectNext),
        scheduler=scheduler,
    )
    result = MapObservable(obs, lambda t2: t2[0])
    return result

