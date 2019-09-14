from typing import Any

from rxbp.ack.ackimpl import continue_ack
from rxbp.observablesubjects.cacheservefirstosubject import CacheServeFirstOSubject
from rxbp.subjects.subjectbase import SubjectBase
from rxbp.subscriber import Subscriber
from rxbp.subscription import Subscription, SubscriptionInfo


class Subject(SubjectBase):
    def __init__(self):
        super().__init__()

        self._obs_subject = None

    def unsafe_subscribe(self, subscriber: Subscriber) -> Subscription:
        self._obs_subject = CacheServeFirstOSubject(scheduler=subscriber.scheduler)
        return Subscription(SubscriptionInfo(base=None), self._obs_subject)

    def on_next(self, elem: Any):
        def gen_val():
            yield elem

        if self._obs_subject is not None:
            return self._obs_subject.on_next(gen_val)
        else:
            return continue_ack

    def on_error(self, exc: Exception):
        if self._obs_subject is not None:
            self._obs_subject.on_error(exc)

    def on_completed(self):
        if self._obs_subject is not None:
            self._obs_subject.on_completed()
