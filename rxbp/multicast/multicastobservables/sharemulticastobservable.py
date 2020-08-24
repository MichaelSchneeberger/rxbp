# @dataclass
# class ShareMultiCastObservable(MultiCastObservableMixin):
#     source: MultiCastObservableMixin
#
#     def observe(self, observer_info: MultiCastObserverInfo) -> Disposable:
#         @dataclass
#         class ShareMultiCastObserver(MultiCastObserverMixin):
#             source: MultiCastObserverMixin
#             raise_exception: Callable[[Callable[[], None]], None]
#
#             def on_next(self, elem: MultiCastValue) -> None:
#                 observer.on_next(x)
#                 observer.on_completed()
#
#             def on_error(self, exc: Exception) -> None:
#                 self.source.on_error(exc)
#
#             def on_completed(self) -> None:
#                 def func():
#                     raise SequenceContainsNoElementsError()
#
#                 try:
#                     self.raise_exception(func)
#                 except Exception as exc:
#                     observer.on_error(exc)
#
#         observer = ShareMultiCastObserver(
#             source=observer_info.observer,
#             raise_exception=self.raise_exception,
#         )
#
#         return self.source.observe(observer_info.copy(observer))
