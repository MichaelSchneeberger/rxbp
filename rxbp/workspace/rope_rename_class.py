

import rope.base.project
import rope.refactor.rename
import rope.refactor.restructure

from_ = 'rxbp.flowabletree.nodes'

old_name = 'TwoChildrenStateMonadNode'
new_name = 'TwoChildrenFlowableNode'

proj = rope.base.project.Project('.')

mod = proj.get_module(f'{from_}')

name = mod.get_attribute(old_name)
pymod, lineno = name.get_definition_location()
lineno_start, lineno_end = pymod.logical_lines.logical_line_in(lineno)
offset = pymod.resource.read().index(name.pyobject.get_name(), pymod.lines.get_line_start(lineno))

change = rope.refactor.rename.Rename(proj, pymod.get_resource(), offset).get_changes(new_name)

# print(change.get_description())
change.do()
