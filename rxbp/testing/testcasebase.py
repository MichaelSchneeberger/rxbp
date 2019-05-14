import unittest


class TestCaseBase(unittest.TestCase):
    @staticmethod
    def gen_single(v):
        def gen():
            yield v

        return gen

    @staticmethod
    def gen_seq(v):
        def gen():
            yield from v

        return gen
