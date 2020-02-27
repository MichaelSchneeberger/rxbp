from rxbp.multicast.multicast import MultiCast


class LiftedMultiCast(MultiCast):
    def share(self):
        return self._share()
