from rx.disposables import CompositeDisposable, SingleAssignmentDisposable
from rx.internal import extensionmethod

from rxbackpressure.backpressuretypes.backpressuregreadily import \
    BackpressureGreadily
from rxbackpressure.backpressuretypes.flatmapbackpressure import FlatMapBackpressure
from rxbackpressure.core.anonymousbackpressureobservable import \
    AnonymousBackpressureObservable
from rxbackpressure.core.subflowobservable import SubFlowObservable


@extensionmethod(SubFlowObservable)
def flat_map(self, selector):

    def subscribe_func(observer, scheduler):
        sub_backpressure = FlatMapBackpressure(scheduler=scheduler)
        group = CompositeDisposable()
        is_stopped = [False]
        m = SingleAssignmentDisposable()
        group.add(m)
        parent_scheduler = scheduler

        def on_next(value):
            # print('flat map on next {}'.format(value))
            try:
                source = selector(value)
            except Exception as err:
                observer.on_error(err)
            else:
                inner_subscription = SingleAssignmentDisposable()
                group.add(inner_subscription)

                def on_completed():
                    group.remove(inner_subscription)
                    if is_stopped[0] and len(group) == 1:
                        observer.on_completed()
                        sub_backpressure.on_completed()
                    # else:
                    #     pass

                def subscribe_bp_func(backpressure, scheduler=None):
                    disposable = sub_backpressure.add_backpressure(backpressure, parent_scheduler)
                    return disposable

                # print('subscribe inner')
                source.subscribe(subscribe_bp=subscribe_bp_func, on_next=observer.on_next, on_error=observer.on_error,
                                 on_completed=on_completed, scheduler=scheduler)

        def on_completed():
            is_stopped[0] = True
            if len(group) == 1:
                observer.on_completed()
                sub_backpressure.on_completed()

        def subscribe_bp_func(backpressure, scheduler=None):
            sub_backpressure.backpressure = backpressure
            disposable = observer.subscribe_backpressure(sub_backpressure, parent_scheduler)
            sub_backpressure.disposable.disposable = disposable

            disposable = BackpressureGreadily.apply(backpressure, scheduler2=parent_scheduler)
            return disposable

        m.disposable = self.subscribe(subscribe_bp=subscribe_bp_func,
                                      on_next=on_next, on_error=observer.on_error, on_completed=on_completed,
                                      scheduler=scheduler)
        return group

    return AnonymousBackpressureObservable(subscribe_func=subscribe_func)
