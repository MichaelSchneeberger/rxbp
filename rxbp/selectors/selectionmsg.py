class SelectionMsg:
    pass


class SelectCompleted(SelectionMsg):
    pass


class SelectNext(SelectionMsg):
    pass


select_completed = SelectCompleted()
select_next = SelectNext()