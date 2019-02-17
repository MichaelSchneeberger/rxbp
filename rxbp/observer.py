class Observer:
    def on_next(self, v):
        raise NotImplementedError

    def on_error(self, err):
        raise NotImplementedError

    def on_completed(self):
        raise NotImplementedError
