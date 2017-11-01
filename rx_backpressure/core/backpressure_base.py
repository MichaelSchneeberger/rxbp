from rx_backpressure.internal.blocking_future import BlockingFuture


class BackpressureBase:
    def request(self, number_of_items) -> BlockingFuture:
        # returns a future of the number of items send
        raise NotImplementedError