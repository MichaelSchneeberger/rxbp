from typing import Dict, Any

from rxbp.flowable import Flowable
from rxbp.multicast.flowablestatemixin import FlowableStateMixin


class FlowableDict(FlowableStateMixin):
    def __init__(self, states: Dict[Any, Flowable]):
        self._states = states

    def __getitem__(self, item):
        return self._states[item]

    def __setitem__(self, key, value):
        self._states[key] = value

    def get_flowable_state(self):
        return self._states

    def set_flowable_state(self, states):
        return FlowableDict(states)