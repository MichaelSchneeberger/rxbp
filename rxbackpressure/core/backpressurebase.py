class BackpressureBase:
    def request(self, number_of_items):
        # returns a future of the number of items send
        raise NotImplementedError