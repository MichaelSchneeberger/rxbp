from abc import ABC


class FlatMapStates:
    class State(ABC):
        pass

    class InitialState(State):
        pass

    class WaitOnOuter(State):
        pass

    class Active(State):
        """ Outer value received, inner observable possibly already subscribed
        """

        pass

    class OnOuterCompleted(State):
        # outer observer does not complete the output observer (except in WaitOnNextChild); however, it signals
        #   that output observer should be completed if inner observer completes
        pass

    class OnOuterException(State):
        def __init__(self, exc: Exception):
            self.exc = exc

    class Stopped(State):
        """ either inner observable completed or raise exception
        """
        pass
