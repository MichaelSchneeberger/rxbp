from rx.subjects import AsyncSubject

class Success:
    def __init__(self, next):
        self.next = next

class Failure:
    pass


class Ack(AsyncSubject):
    def merge_ack(self, ack2: 'Ack'):
        if isinstance(ack2, Continue):
            return_ack = self
        elif isinstance(ack2, Stop):
            return_ack = ack2
        else:
            return_ack = Ack()

            def _(v1, v2):
                if isinstance(v1, Stop) or isinstance(v2, Stop):
                    return Stop()
                else:
                    return Continue()

            self.zip(ack2, _).subscribe(return_ack)

        return return_ack

    def connect_ack(self, next_ack: 'Ack'):
        self.subscribe(next_ack)

    def connect_ack_2(self, ack2: 'Ack', out_ack: 'Ack'):
        if isinstance(ack2, Stop):
            out_ack.on_next(ack2)
            out_ack.on_completed()
        elif isinstance(ack2, Continue):
            self.subscribe(out_ack)
        else:
            def _(v1, v2):
                if isinstance(v1, Stop) or isinstance(v2, Stop):
                    raise NotImplementedError
                    # return Stop()
                else:
                    return Continue()

            self.zip(ack2, _).subscribe(out_ack)


class Continue(Ack):
    def __init__(self):
        super().__init__()

        self.has_value = True
        self.value = self
        self.is_stopped = True

    def merge_ack(self, ack2: 'Ack'):
        return ack2

    def connect_ack(self, next_ack: 'Ack'):
        self.subscribe(next_ack)

    def connect_ack_2(self, ack2: 'Ack', out_ack: 'Ack'):
        ack2.subscribe(out_ack)


continue_ack = Continue()

class Stop(Ack):
    def __init__(self):
        super().__init__()

        self.has_value = True
        self.value = self
        self.is_stopped = True

    def merge_ack(self, ack2: 'Ack'):
        return self

    def connect_ack_2(self, ack2: 'Ack', out_ack: 'Ack'):
        self.subscribe(out_ack)

stop_ack = Stop()