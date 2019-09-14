from . import op
from . import stream
# from . import source
from .source import defer, from_iterable, from_range, from_list, return_value, from_rx, concat, share, zip, match, \
    merge, empty


from_ = from_iterable
range = from_range
range_ = from_range

now = return_value
just = return_value
