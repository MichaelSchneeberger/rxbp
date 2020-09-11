from dataclasses import dataclass

import rx
from rx.disposable import CompositeDisposable

from rxbp.init.initobserverinfo import init_observer_info
from rxbp.multicast.multicastobservable import MultiCastObservable
from rxbp.multicast.multicastobserverinfo import MultiCastObserverInfo
from rxbp.multicast.multicastobservers.fromflowablemulticastobserver import FromFlowableMultiCastObserver
from rxbp.multicast.multicastsubscriber import MultiCastSubscriber
from rxbp.observable import Observable


@dataclass
class FromFlowableMultiCastObservable(MultiCastObservable):
    source: Observable
    subscriber: MultiCastSubscriber

    def observe(self, observer_info: MultiCastObserverInfo) -> rx.typing.Disposable:
        composite_disposable = CompositeDisposable()
        disposable = self.source.observe(init_observer_info(
            observer=FromFlowableMultiCastObserver(
                next_observer=observer_info.observer,
                subscriber=self.subscriber,
                composite_disposable=composite_disposable,
            )
        ))
        composite_disposable.add(disposable)
        return composite_disposable
