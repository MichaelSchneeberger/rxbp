import rx
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

            rx.zip(self, ack2).pipe(rx.operators.map(lambda t2: _(*t2))).subscribe(return_ack)

        return return_ack

    def connect_ack(self, next_ack: 'Ack'):
        self.subscribe(next_ack)

    def connect_ack_2(self, ack2: 'Ack', next_ack: 'Ack'):
        if isinstance(ack2, Stop):
            next_ack.on_next(ack2)
            next_ack.on_completed()
        elif isinstance(ack2, Continue):
            self.subscribe(next_ack)
        else:
            def _(v1, v2):
                if isinstance(v1, Stop) or isinstance(v2, Stop):
                    raise NotImplementedError
                    # return Stop()
                else:
                    return Continue()

            self.zip(ack2, _).subscribe(next_ack)

    def debug(self, name):
        ack = Ack()
        self.do_action(lambda v: print('{} {}'.format(name, v))).subscribe(ack)
        return ack


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

    def connect_ack_2(self, ack2: 'Ack', next_ack: 'Ack'):
        ack2.subscribe(next_ack)


continue_ack = Continue()


class Stop(Ack):
    def __init__(self):
        super().__init__()

        self.has_value = True
        self.value = self
        self.is_stopped = True

    def merge_ack(self, ack2: 'Ack'):
        return self

    def connect_ack_2(self, ack2: 'Ack', next_ack: 'Ack'):
        self.subscribe(next_ack)


stop_ack = Stop()