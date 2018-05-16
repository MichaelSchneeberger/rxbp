from typing import Callable, Any

from rxbackpressure.core.observablebase import ObservableBase
# from rxbackpressure.linq.first import first_or_default_async
# from rxbackpressure.linq.map import map_operator
# from rxbackpressure.linq.mapcount import map_count_operator
# from rxbackpressure.linq.pairwise import pairwise_operator
# from rxbackpressure.linq.tolist import to_list_operator
# from rxbackpressure.linq.toobservable import to_observable_operator
# from rxbackpressure.linq.window import window_operator
# from rxbackpressure.linq.zip import zip_operator


class BackpressureObservable(ObservableBase):
    # def map(self, selector):
    #     # return map_operator(self, selector)
    #     pass
    #
    # def map_count(self, selector):
    #     # return map_count_operator(self, selector)
    #     pass
    #
    # # def multicast(self, subject=None):
    # #     subject = subject or SyncedSubject()
    # #     return ConnectableBackpressureObservable(self, subject)
    #
    # def pairwise(self):
    #     # return pairwise_operator(self)
    #     pass
    #
    # def to_list(self):
    #     # return to_list_operator(self)
    #     pass
    #
    # def to_observable(self, scheduler=None):
    #     # return to_observable_operator(self, scheduler=scheduler)
    #     pass
    #
    # def window(self,
    #            other: 'BackpressureObservable',
    #            is_lower: Callable[[Any, Any], bool],
    #            is_higher: Callable[[Any, Any], bool]) -> 'BackpressureObservable':
    #     # return window_operator(self, other, is_lower, is_higher)
    #     pass
    #
    # def zip(self, other, selector=lambda v1, v2: (v1, v2)):
    #     # return zip_operator(self, other, selector=lambda v1, v2: (v1, v2))
    #     pass
    #
    # def first(self, predicate=None, default_value=None):
    #     # return first_or_default_async(self, True, default_value)
    #     pass
    pass