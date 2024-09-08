from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from donotation import do

import continuationmonad
from continuationmonad.abc import Cancellable
from continuationmonad.typing import ContinuationCertificate, ContinuationMonad

from rxbp.flowabletree.data.observer import Observer
from rxbp.flowabletree.nodes import SingleChildFlowableNode
from rxbp.flowabletree.data.observeresult import ObserveResult
from rxbp.state import State



@dataclass
class SharedCancellable(Cancellable):
    shared: SharedObserver

    def cancel(self, certificate: ContinuationCertificate):
        self.shared.cancellable_counter = self.shared.cancellable_counter - 1
        if self.shared.cancellable_counter == 0:
            self.shared.cancellable.cancel(certificate)
        else:
            certificate.verify()

# @dataclass
# class SharedMemory:
#     cancellable: Cancellable
#     cancellable_counter: int


@dataclass
class SharedObserver[V](Observer[V]):
    downstream: list[Observer]
    cancellable: Cancellable
    cancellable_counter: int

    @do()
    def on_next(self, value: V):

        def gen_ack_continuations():
            for observer in self.downstream:
                yield observer.on_next(value)

        return continuationmonad.join(
            continuations=gen_ack_continuations(),
        )

    @do()
    def _complete_stream(self, continuations: Iterable[ContinuationMonad]):
        first, *others = yield from continuationmonad.join(continuations)

        def verify_others():
            for certificate in others:
                assert certificate.verify()

        verify_others()

        return continuationmonad.from_(first)

    def on_next_and_complete(self, value: V):
        def gen_certificates():
            for observer in self.downstream:
                yield observer.on_next_and_complete(value)

        return self._complete_stream(gen_certificates())

    def on_completed(self):
        def gen_certificates():
            for observer in self.downstream:
                yield observer.on_completed()

        return self._complete_stream(gen_certificates())
    
    def on_error(self, exception: Exception):
        def gen_certificates():
            for observer in self.downstream:
                yield observer.on_error(exception)

        return self._complete_stream(gen_certificates())



class Shared[V](SingleChildFlowableNode[V, V]):
    def unsafe_subscribe(
        self, state: State, observer: Observer[V],
    ) -> tuple[State, ObserveResult]:
        if self in state.shared_observables:
            shared = state.shared_observables[self]
            shared.downstream.append(observer)
            n_state = state

            # generate additional certificates (there is no better solution)
            certificate = state.subscription_trampoline._create_certificate() # type: ignore
        
        else:

            shared = SharedObserver(
                downstream=[observer],
                cancellable=None,  # type: ignore
                cancellable_counter=0,
            )

            state.shared_observables[self] = shared

            n_state, result = self.child.unsafe_subscribe(state, shared)
            shared.cancellable = result.cancellable

            certificate = result.certificate
            
        shared.cancellable_counter += 1
        cancellable = SharedCancellable(shared=shared)
        return n_state, ObserveResult(
            cancellable=cancellable,
            certificate=certificate,
        )

