"""
Tests for the scoring engine.
Run with: pytest tests/test_scoring.py -v
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from unittest.mock import MagicMock
from scoring.scorer import Scorer


def make_lead(**kwargs):
    """Create a mock Lead object with given attributes."""
    lead = MagicMock()
    # Defaults
    lead.id = kwargs.get("id", 1)
    lead.title = kwargs.get("title", "T2 Lisboa centro")
    lead.description = kwargs.get("description", "")
    lead.price = kwargs.get("price", 200_000)
    lead.area_m2 = kwargs.get("area_m2", 80)
    lead.price_per_m2 = kwargs.get("price_per_m2", 2_500)
    lead.price_benchmark = kwargs.get("price_benchmark", 4_500)
    lead.price_delta_pct = kwargs.get("price_delta_pct", 44.4)  # cheap
    lead.zone = kwargs.get("zone", "Lisboa")
    lead.typology = kwargs.get("typology", "T2")
    lead.is_owner = kwargs.get("is_owner", True)
    lead.contact_phone = kwargs.get("contact_phone", "+351912345678")
    lead.contact_email = kwargs.get("contact_email", None)
    lead.days_on_market = kwargs.get("days_on_market", 0)
    lead.agency_name = kwargs.get("agency_name", None)
    return lead


class TestScorer:
    scorer = Scorer()

    # ── Price opportunity ────────────────────────────────────────────────────

    def test_price_far_below_benchmark_scores_30(self):
        lead = make_lead(price_delta_pct=25.0)
        result = self.scorer.score(lead)
        assert result.breakdown["price_opportunity"] == 30

    def test_price_10_to_20_below_scores_20(self):
        lead = make_lead(price_delta_pct=15.0)
        result = self.scorer.score(lead)
        assert result.breakdown["price_opportunity"] == 20

    def test_price_5_to_10_below_scores_10(self):
        lead = make_lead(price_delta_pct=7.0)
        result = self.scorer.score(lead)
        assert result.breakdown["price_opportunity"] == 10

    def test_price_above_market_scores_0(self):
        lead = make_lead(price_delta_pct=-5.0)
        result = self.scorer.score(lead)
        assert result.breakdown["price_opportunity"] == 0

    def test_no_price_delta_scores_0(self):
        lead = make_lead(price_delta_pct=None)
        result = self.scorer.score(lead)
        assert result.breakdown["price_opportunity"] == 0

    # ── Urgency signals ──────────────────────────────────────────────────────

    def test_urgency_keyword_detected(self):
        lead = make_lead(description="Vendo urgente! Preciso de liquidez imediata.")
        result = self.scorer.score(lead)
        assert result.breakdown["urgency_signals"] == 25

    def test_heranca_keyword(self):
        lead = make_lead(description="Venda por herança. Partilha familiar.")
        result = self.scorer.score(lead)
        assert result.breakdown["urgency_signals"] == 20

    def test_no_urgency_scores_0(self):
        lead = make_lead(description="Bonito apartamento com garagem e jardim.")
        result = self.scorer.score(lead)
        assert result.breakdown["urgency_signals"] == 0

    # ── Owner direct ─────────────────────────────────────────────────────────

    def test_direct_owner_scores_20(self):
        lead = make_lead(is_owner=True)
        result = self.scorer.score(lead)
        assert result.breakdown["owner_direct"] == 20

    def test_agency_with_contact_scores_10(self):
        lead = make_lead(is_owner=False, contact_phone="+351912345678")
        result = self.scorer.score(lead)
        assert result.breakdown["owner_direct"] == 10

    def test_agency_no_contact_scores_0(self):
        lead = make_lead(is_owner=False, contact_phone=None, contact_email=None)
        result = self.scorer.score(lead)
        assert result.breakdown["owner_direct"] == 0

    # ── Days on market ───────────────────────────────────────────────────────

    def test_over_90_days_scores_15(self):
        lead = make_lead(days_on_market=120)
        result = self.scorer.score(lead)
        assert result.breakdown["days_on_market"] == 15

    def test_60_to_90_days_scores_10(self):
        lead = make_lead(days_on_market=75)
        result = self.scorer.score(lead)
        assert result.breakdown["days_on_market"] == 10

    def test_under_30_days_scores_0(self):
        lead = make_lead(days_on_market=10)
        result = self.scorer.score(lead)
        assert result.breakdown["days_on_market"] == 0

    # ── Data quality ─────────────────────────────────────────────────────────

    def test_full_data_scores_5(self):
        lead = make_lead(contact_phone="+351912345678", area_m2=80, zone="Lisboa", price=200_000)
        result = self.scorer.score(lead)
        assert result.breakdown["data_quality"] == 5

    def test_missing_contact_scores_lower(self):
        lead = make_lead(contact_phone=None, contact_email=None, area_m2=80, zone="Lisboa")
        result = self.scorer.score(lead)
        assert result.breakdown["data_quality"] <= 1

    # ── Zone priority ────────────────────────────────────────────────────────

    def test_lisboa_scores_5(self):
        lead = make_lead(zone="Lisboa")
        result = self.scorer.score(lead)
        assert result.breakdown["zone_priority"] == 5

    def test_cascais_scores_5(self):
        lead = make_lead(zone="Cascais")
        result = self.scorer.score(lead)
        assert result.breakdown["zone_priority"] == 5

    def test_seixal_scores_2(self):
        lead = make_lead(zone="Seixal")
        result = self.scorer.score(lead)
        assert result.breakdown["zone_priority"] == 2

    # ── Labels ───────────────────────────────────────────────────────────────

    def test_hot_label_above_75(self):
        lead = make_lead(
            price_delta_pct=25,
            description="Vendo urgente herança",
            is_owner=True,
            days_on_market=100,
            zone="Lisboa",
            contact_phone="+351912345678",
            area_m2=80,
        )
        result = self.scorer.score(lead)
        assert result.label == "HOT"
        assert result.total >= 75

    def test_cold_label_below_50(self):
        lead = make_lead(
            price_delta_pct=-10,  # above market
            description="",
            is_owner=False,
            contact_phone=None,
            contact_email=None,
            days_on_market=0,
            zone="Seixal",
            area_m2=None,
            price=None,
        )
        result = self.scorer.score(lead)
        assert result.label == "COLD"
        assert result.total < 50

    # ── Score clamped 0-100 ──────────────────────────────────────────────────

    def test_score_never_exceeds_100(self):
        lead = make_lead(
            price_delta_pct=99,
            description="urgente herança divórcio banco execução emigração",
            is_owner=True,
            days_on_market=999,
            zone="Lisboa",
            contact_phone="+351912345678",
            area_m2=80,
        )
        result = self.scorer.score(lead)
        assert result.total <= 100

    def test_score_never_below_0(self):
        lead = make_lead(price_delta_pct=-100)
        result = self.scorer.score(lead)
        assert result.total >= 0
