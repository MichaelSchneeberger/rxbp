from rx.disposable import Disposable
from rxbp.flowablebase import FlowableBase
from rxbp.observable import Observable
from rxbp.observer import Observer
from rxbp.subjects.cacheservefirstsubject import CacheServeFirstSubject
from rxbp.subscriber import Subscriber


class Service:
    def __init__(self):
        self.handler = None

    def register(self, handler):
        self.handler = handler

    def request(self):
        pass

    def send(self, value):
        self.handler(value)

    def unregister(self):
        self.handler = None


class MyFlowable(FlowableBase):
    def unsafe_subscribe(self, subscriber: Subscriber) -> FlowableBase.FlowableReturnType:
        subject = CacheServeFirstSubject(scheduler=subscriber.scheduler)
