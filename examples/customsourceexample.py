"""
This example demonstrates a use-case of a Subject.

Subjects are only then needed, if it cannot be achieved with the rxbp sources and
operators. When using Subjects, we actually build our own rxbp source or operator
extension if we want it or not. And therefore, we are responsible to correctly
apply `on_next`, `on_completed` and `on_error`  downstream function, as well as
react to a dispose call. It is important to be aware of this distinction!
"""

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
