from typing import Dict, Any

from rxbp.flowable import Flowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin


class FlowableDict(FlowableStateMixin):
    def __init__(self, states: Dict[Any, Flowable] = None):
        self._states = states or {}

    def __add__(self, key_value: Dict[Any, Flowable]) -> 'FlowableDict':
        states = {**self._states, **key_value}
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