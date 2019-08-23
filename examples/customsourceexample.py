from rxbp.ack.single import Single
from rxbp.subjects.subject import Subject


class Service:
    def __init__(self):
        self.handler = None

    def register(self, handler):
        self.handler = handler

    def request(self):
        print('request')

    def send(self, value):
        self.handler(value)

    def unregister(self):
        self.handler = None


service = Service()

# build flowable and subscribe
# ----------------------------

subject = Subject()
subject.subscribe(print)


# connect to Service
# ------------------

def handler(value):
    ack = subject.on_next(value)

    class _(Single):

        def on_next(self, elem):
            service.request()

        def on_error(self, exc: Exception):
            pass

    ack.subscribe(_())

service.register(handler=handler)
service.request()

# simulate service
# ----------------

service.send(0)
service.send(1)
service.send(3)

service.unregister()
