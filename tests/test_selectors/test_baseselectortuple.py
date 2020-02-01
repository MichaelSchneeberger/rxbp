import unittest
from typing import Optional

from rxbp.observables.refcountobservable import RefCountObservable
from rxbp.selectors.baseandselectors import BaseAndSelectors, BaseSelectorsAndSelectorMaps
from rxbp.selectors.bases.numericalbase import NumericalBase
from rxbp.selectors.observables.mergeselectorobservable import MergeSelectorObservable
from rxbp.selectors.selectormap import IdentitySelectorMap, ObservableSelectorMap
from rxbp.subscriber import Subscriber
from rxbp.testing.testobservable import TestObservable
from rxbp.testing.testscheduler import TestScheduler
from tests.test_selectors.testbase import TestBase


class TestBaseSelectorTuple(unittest.TestCase):
    def setUp(self):
        self.scheduler = TestScheduler()
        self.subscriber = Subscriber(
            scheduler=self.scheduler,
            subscribe_scheduler=self.scheduler,
        )
        self.num_base_1 = NumericalBase(1)
        self.num_base_2 = NumericalBase(2)
        self.num_base_3 = NumericalBase(3)
        self.sel2 = TestObservable()
        self.sel3 = TestObservable()

    def test_match_two_bases_by_identity(self):
        t1 = BaseAndSelectors(base=NumericalBase(1), selectors={2: None})
        t2 = BaseAndSelectors(base=NumericalBase(1), selectors={3: None})

        result: Optional[BaseSelectorsAndSelectorMaps] = t1.get_selectors(t2, self.subscriber)

        self.assertIsInstance(result, BaseSelectorsAndSelectorMaps)
        self.assertIsInstance(result.right, IdentitySelectorMap)
        self.assertIsInstance(result.left, IdentitySelectorMap)
        self.assertIsInstance(result.base_selectors.base, NumericalBase)
        self.assertEqual(1, result.base_selectors.base.num)
        self.assertEqual({2: None, 3: None}, result.base_selectors.selectors)

    def test_match_by_using_one_selector_by_identity_to_left_base(self):

        t1 = BaseAndSelectors(base=self.num_base_1, selectors={self.num_base_2: self.sel2})
        t2 = BaseAndSelectors(base=self.num_base_2, selectors={self.num_base_3: self.sel3})

        result: Optional[BaseSelectorsAndSelectorMaps] = t1.get_selectors(t2, self.subscriber)

        # test selector maps
        # ------------------

        self.assertIsInstance(result, BaseSelectorsAndSelectorMaps)
        self.assertIsInstance(result.right, ObservableSelectorMap)
        self.assertIsInstance(result.left, IdentitySelectorMap)

        self.assertEqual(self.sel2, result.right.observable)

        # test new base
        # -------------

        self.assertEqual(self.num_base_1, result.base_selectors.base)

        # test selectors
        # --------------

        # selectors from the
        self.assertIn(self.num_base_2, result.base_selectors.selectors)
        self.assertEqual(self.sel2, result.base_selectors.selectors[self.num_base_2])

        self.assertIn(self.num_base_3, result.base_selectors.selectors)
        selector_3 = result.base_selectors.selectors[self.num_base_3]
        self.assertIsInstance(selector_3, RefCountObservable)
        self.assertIsInstance(selector_3.source, MergeSelectorObservable)
        self.assertEqual(self.sel3, selector_3.source.left_observable)
        self.assertEqual(self.sel2, selector_3.source.right_observable)

    def test_match_by_using_one_selector_by_non_identity_to_left_base(self):
        base_a = TestBase(BaseAndSelectors(
            base=self.num_base_2,
            selectors={self.num_base_3: self.sel3}
        ))

        base_b = TestBase(BaseAndSelectors(
            base=self.num_base_3,
        ))


        t1 = BaseAndSelectors(base=self.num_base_1, selectors={base_a: self.sel2})
        t2 = BaseAndSelectors(base=base_b)

        result: Optional[BaseSelectorsAndSelectorMaps] = t1.get_selectors(t2, self.subscriber)

        # test selector maps
        # ------------------

        self.assertIsInstance(result, BaseSelectorsAndSelectorMaps)
        self.assertIsInstance(result.left, IdentitySelectorMap)
        self.assertIsInstance(result.right, ObservableSelectorMap)

        right_map = result.right
        self.assertIsInstance(right_map, ObservableSelectorMap)
        self.assertIsInstance(right_map.observable, RefCountObservable)
        self.assertIsInstance(right_map.observable.source, MergeSelectorObservable)
        self.assertEqual(right_map.observable.source.left_observable, self.sel3)
        self.assertEqual(right_map.observable.source.right_observable, self.sel2)

        # self.assertEqual(self.sel2, result.right.observable)

        # test new base
        # -------------

        # self.assertEqual(self.num_base_1, result.base_selectors.base)

        # # test selectors
        # # --------------
        #
        # # # selectors from the
        # self.assertIn(base_a, result.base_selectors.selectors)
        # self.assertEqual(self.sel2, result.base_selectors.selectors[base_a])

    # def test_match_by_using_one_selector_to_unknown_base(self):
    #
    #
    #
    #     t1 = BaseAndSelectors(base=TestBase(self.num_base_1), selectors={self.num_base_2: self.sel2})
    #     t2 = BaseAndSelectors(base=self.num_base_2, selectors={self.num_base_3: self.sel3})

    #     result: Optional[BaseSelectorAndSelectorMaps] = t1.get_selectors(t2, self.subscriber)
    #
    #     self.assertIsInstance(result, BaseSelectorAndSelectorMaps)
    #     self.assertIsInstance(result.right, IdentitySelectorMap)
    #     self.assertIsInstance(result.left, IdentitySelectorMap)
    #     self.assertIsInstance(result.base_selectors.base, NumericalBase)
    #     self.assertEqual(1, result.base_selectors.base.num)
    #     self.assertEqual({2: None, 3: None}, result.base_selectors.selectors)

    def test_not_matching(self):
        t1 = BaseAndSelectors(base=NumericalBase(1))
        t2 = BaseAndSelectors(base=NumericalBase(2))

        result: Optional[BaseSelectorsAndSelectorMaps] = t1.get_selectors(t2, self.subscriber)

        self.assertIsNone(result)
