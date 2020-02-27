import unittest

from rxbp.flowables.concatflowable import ConcatFlowable
from rxbp.selectors.bases.concatbase import ConcatBase


class TestConcatBase(unittest.TestCase):
    def initialize(self):
        ConcatBase
