import rope.base.project
import rope.refactor.rename
import rope.refactor.restructure

from_ = 'sosopt.constraintsprovider'

to_ = 'constraints'

proj = rope.base.project.Project('.')

res = proj.get_module(f'{from_}').get_resource()

change = rope.refactor.rename.Rename(proj, res).get_changes(to_)

# print(change.get_description())
# change.do()
