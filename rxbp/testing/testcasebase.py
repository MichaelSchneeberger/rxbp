import unittest


class TestCaseBase(unittest.TestCase):
    @staticmethod
    def gen_single(v):
        def gen():
            yield v

        return gen
