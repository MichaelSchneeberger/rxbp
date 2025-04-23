# Contract

Most points in the contract are taken from the Monix documentation:

- Grammar: on_next CAN be called zero, one or multiple times, 
followed by an optional on_complete or on_error if the stream is finite, 
so in other words on_next* (on_complete | on_error)?. And once a 
final event happens, either on_complete or on_error, then no 
further calls are allowed.
- Back-pressure: each on_next call MUST wait on a Continue result 
returned by the Ack of the previous on_next call.
- Back-pressure for on_complete and on_error is optional: when calling
on_complete or on_error you are not required to wait on the Ack 
of the previous on_next.
- Stream cancellation: on_next calls can return Stop and after 
receiving it the data-source MUST no longer send any events. 
Tied with the back-pressure requirement, it means that cancellation 
by means of Stop is always deterministic and immediate.
- Ordering/non-concurrent guarantee: calls to on_next, on_complete and 
on_error MUST BE ordered and thus non-concurrent. As a consequence 
Observer implementations don’t normally need to synchronize their 
on_next, on_complete or on_error methods.
- Exactly once delivery for final events: you are allowed to call 
either on_complete or on_error at most one time. And you cannot call 
both on_complete and on_error.
- The implementation of on_next, on_error or on_complete MUST NOT throw 
exceptions. Never throw exceptions in their implementation and protect 
against code that might do that.
- The on_next method takes either a list or an iterator of values 
as an argument. If an iterator is given and the on_next method iterates
over the elements, it must catch a possible exception when iterating over
the elements.
