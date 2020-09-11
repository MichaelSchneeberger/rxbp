from . import op
from .source import empty, join_flowables, merge, from_iterable, \
    build_imperative_multicast, return_value, from_flowable

from_ = from_iterable
just = return_value
