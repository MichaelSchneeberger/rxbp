from rxbp.flowable import Flowable


class MultiCastFlowable(Flowable):
    def share(self) -> 'MultiCastFlowable':
        return MultiCastFlowable(self._share())
