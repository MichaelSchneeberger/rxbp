from typing import Dict, Any, Union

from rxbp.flowable import Flowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin


class FlowableDict(FlowableStateMixin):
    def __init__(self, states: Dict[Any, Flowable] = None):
        self._states = states or {}

    def __add__(self, other: Union[FlowableStateMixin, Dict[Any, Flowable]]) -> 'FlowableDict':
        if isinstance(other, FlowableStateMixin):
            state = other.get_flowable_state()
        elif isinstance(other, dict):
            state = other
        else:
            Exception(f'illegal value "{other}"')

        states = {**self._states, **state}
        return FlowableDict(states)

    def keys(self):
        return list(self._states.keys())

    def __getitem__(self, item):
        return self._states[item]

    def __setitem__(self, key, value):
        self._states[key] = value

    def get_flowable_state(self):
        return self._states

    @staticmethod
    def set_flowable_state(states: Dict[Any, Flowable]):
        return FlowableDict(states)