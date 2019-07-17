from . import op
from .sources import defer, from_iterable, from_range, from_list, return_value, from_rx, concat, share, zip, match, merge


from_ = from_iterable
range = from_range
range_ = from_range

now = return_value
just = return_value
