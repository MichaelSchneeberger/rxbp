from traceback import FrameSummary
from typing import List

from rxbp.indexed.observables.controlledzipindexedobservable import ControlledZipIndexedObservable
from rxbp.indexed.selectors.observables.mergeselectorobservable import MergeSelectorObservable
from rxbp.indexed.selectors.selectnext import SelectNext
from rxbp.indexed.selectors.selectcompleted import SelectCompleted
from rxbp.observable import Observable
from rxbp.observables.controlledzipobservable import ControlledZipObservable
from rxbp.observables.mapobservable import MapObservable
from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.observablesubjects.publishobservablesubject import PublishObservableSubject
from rxbp.scheduler import Scheduler


def merge_selectors(
        left: Observable,
        right: Observable,
        subscribe_scheduler: Scheduler,
        stack: List[FrameSummary],
):
    """
    SC RL SN RL SN--->SN--->SN RL SC RL
    SC--->SC RR SN RR SN RR SC RR SN--->SN
    ok    -     ok    ok    -     ok
    SC          SN    SN          SC

    SC RL SC RL SC
    SC--->SC--->SC
          SC
    """

    obs = MergeSelectorObservable(
        left=left,
        right=right,
        # scheduler=subscribe_scheduler,
    )

    o3 = RefCountObservable(
        source=obs,
        subject=PublishObservableSubject(),
        subscribe_scheduler=subscribe_scheduler,
        stack=stack,
    )

    return o3


def select_observable(
        obs: Observable,
        selector: Observable,
        scheduler: Scheduler,
        stack: List[FrameSummary],
):
    def request_left(left, right):
        return isinstance(right, SelectCompleted)

    result = MapObservable(
        source=ControlledZipObservable(
            left=obs,
            right=selector,
            request_left=request_left,
            request_right=lambda l, r: True,
            match_func=lambda l, r: isinstance(r, SelectNext),
            scheduler=scheduler,
            stack=stack,
        ),
        func=lambda t2: t2[0],
    )
    return result
