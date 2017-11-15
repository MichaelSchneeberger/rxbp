import rx

from rxbackpressure.internal.blockingfuture import BlockingFuture
from . import linq

rx.config['Future'] = BlockingFuture