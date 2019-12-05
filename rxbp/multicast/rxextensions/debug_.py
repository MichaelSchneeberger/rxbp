from typing import Callable

from rx import Observable
from rx.disposable import CompositeDisposable, Disposable


def debug(name: str = None) -> Callable[[Observable], Observable]:
    def _(source: Observable):
        def subscribe(observer, scheduler=None):
            print(f'{name} subscribe observer "{observer}"')

            def on_next(v):
                print(f'{name}.on_next({v})')
                observer.on_next(v)

            def on_error(exc):
                print(f'{name}.on_error({exc})')
                observer.on_error(exc)

            def on_completed():
                print(f'{name}.on_completed()')
                observer.on_completed()

            disposable = source.subscribe_(on_next, on_error, on_completed, scheduler)

            return CompositeDisposable(disposable, Disposable(lambda: print(f'{name} disposed')))

        return Observable(subscribe)

    return _
