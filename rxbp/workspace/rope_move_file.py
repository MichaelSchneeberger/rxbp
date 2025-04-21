import rope.base.project
from rope.refactor import move

file_name = 'sparserepr'

from_ = 'polymat.expression'
to_ = 'polymat'

proj = rope.base.project.Project('.')

source = proj.get_module(f'{from_}.{file_name}')

move_module = move.MoveModule(proj, source.get_resource())

dest = proj.get_folder(to_)

change = move_module.get_changes(dest)

# print(change.get_description())
# change.do()
