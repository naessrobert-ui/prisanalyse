import unittest
from newsweb_routes import _private_consensus_headline

class PrivateHeadlineTests(unittest.TestCase):
    def _item(self,kind):
        return {"issuer":"TGS ASA","headline":"TGS rapporterer resultat",
                "lead":"Original AI summary","consensus_kind":kind,
                "consensus_comparison":[{"metric":"ebit","label":"EBIT",
                    "actual":160,"consensus":100,"surprise_pct":60.0}]}
    def test_actual_pre_release_surprise_becomes_headline(self):
        item=self._item("actual")
        _private_consensus_headline(item)
        self.assertIn("60 % over konsensus",item["headline"])
        self.assertIn("160",item["lead"])
    def test_simulated_consensus_never_becomes_factual_headline(self):
        item=self._item("simulation")
        _private_consensus_headline(item)
        self.assertEqual(item["headline"],"TGS rapporterer resultat")
    def test_no_pre_release_comparison_no_headline(self):
        item=self._item("actual")
        item["consensus_comparison"]=[]
        _private_consensus_headline(item)
        self.assertEqual(item["headline"],"TGS rapporterer resultat")
    def test_small_difference_does_not_override_story(self):
        item=self._item("actual")
        item["consensus_comparison"][0]["surprise_pct"]=2.0
        _private_consensus_headline(item)
        self.assertEqual(item["headline"],"TGS rapporterer resultat")
