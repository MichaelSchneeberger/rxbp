import rope.base.project
from rope.refactor.move import MoveGlobal

object_ = 'V'
from_ = 'polymat/statemonad/statemonadmixin'
to_ = 'polymat/statemonad/typing'

proj = rope.base.project.Project('.')

mod = proj.get_module(f'{from_}')

name_attr = mod.get_attribute(object_)

try:
    name = name_attr.pyobject.get_name()
except AttributeError:
    name = object_

pymod, lineno = name_attr.get_definition_location()

lineno_start, lineno_end = pymod.logical_lines.logical_line_in(lineno)
offset = pymod.resource.read().index(name, pymod.lines.get_line_start(lineno))

move_global = MoveGlobal(
    proj, 
    mod.get_resource(), 
    offset,
)

dest_mod = proj.get_module(to_)
change = move_global.get_changes(dest_mod.get_resource())

# print(change.get_description())
# change.do()
