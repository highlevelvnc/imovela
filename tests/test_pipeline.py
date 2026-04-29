"""
Tests for the pipeline: normalizer, deduplicator, enricher.
Run with: pytest tests/test_pipeline.py -v
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from pipeline.normalizer import Normalizer
from pipeline.deduplicator import Deduplicator
from pipeline.enricher import Enricher
from utils.helpers import parse_price, parse_area, clean_phone, extract_typology, normalise_zone, fingerprint


class TestHelpers:

    def test_parse_price_euro_format(self):
        assert parse_price("280.000 €") == 280_000.0

    def test_parse_price_space_separator(self):
        assert parse_price("195 000€") == 195_000.0

    def test_parse_price_comma_decimal(self):
        result = parse_price("195,500 €")
        assert result == 195_500.0

    def test_parse_price_none_on_garbage(self):
        assert parse_price("Preço sob consulta") is None

    def test_parse_price_none_on_empty(self):
        assert parse_price("") is None

    def test_parse_area_m2(self):
        assert parse_area("85 m²") == 85.0

    def test_parse_area_no_space(self):
        assert parse_area("120m2") == 120.0

    def test_parse_area_decimal_comma(self):
        assert parse_area("85,5 m²") == 85.5

    def test_clean_phone_portuguese(self):
        assert clean_phone("912 345 678") == "+351912345678"

    def test_clean_phone_with_country_code(self):
        assert clean_phone("+351 912 345 678") == "+351912345678"

    def test_clean_phone_invalid(self):
        assert clean_phone("abc") is None

    def test_extract_typology_t2(self):
        assert extract_typology("T2 com jardim") == "T2"

    def test_extract_typology_moradia(self):
        assert extract_typology("Moradia V4 com piscina") == "Moradia"

    def test_extract_typology_terreno(self):
        assert extract_typology("Terreno urbano 500m²") == "Terreno"

    def test_normalise_zone_lisboa(self):
        assert normalise_zone("Lisboa, Benfica") == "Lisboa"

    def test_normalise_zone_cascais(self):
        assert normalise_zone("Estoril, Cascais") == "Cascais"

    def test_fingerprint_stable(self):
        fp1 = fingerprint("T2", "Lisboa", "200000", "80")
        fp2 = fingerprint("T2", "Lisboa", "200000", "80")
        assert fp1 == fp2

    def test_fingerprint_different_inputs(self):
        fp1 = fingerprint("T2", "Lisboa", "200000", "80")
        fp2 = fingerprint("T3", "Cascais", "300000", "100")
        assert fp1 != fp2


class TestNormalizer:
    normalizer = Normalizer()

    def test_olx_basic(self):
        raw = {
            "title": "T2 Lisboa Alvalade excelente localização",
            "price_raw": "195.000 €",
            "area_raw": "78 m²",
            "location_raw": "Lisboa, Alvalade",
            "url": "https://olx.pt/d/t2-IDabc123.html",
            "external_id": "abc123",
            "contact_phone": "912345678",
            "description": "Apartamento em excelente estado.",
            "agency_name": "",
        }
        result = self.normalizer.normalize("olx", raw)
        assert result is not None
        assert result["price"] == 195_000.0
        assert result["area_m2"] == 78.0
        assert result["typology"] == "T2"
        assert result["zone"] == "Lisboa"
        assert result["contact_phone"] == "+351912345678"

    def test_olx_no_title_returns_none(self):
        raw = {"title": "", "price_raw": "195.000 €", "url": "http://x.com"}
        result = self.normalizer.normalize("olx", raw)
        assert result is None

    def test_idealista_basic(self):
        raw = {
            "title": "Apartamento T3 em Cascais com vista mar",
            "price_raw": "480.000 €",
            "area_raw": "120 m²",
            "typology_raw": "T3",
            "location_raw": "Cascais, Estoril",
            "url": "https://idealista.pt/imovel/12345/",
            "external_id": "12345",
            "description": "Preciso vender urgente.",
        }
        result = self.normalizer.normalize("idealista", raw)
        assert result is not None
        assert result["typology"] == "T3"
        assert result["zone"] == "Cascais"
        assert result["price"] == 480_000.0

    def test_owner_detection_no_agency(self):
        raw = {
            "title": "T2 vendo directamente",
            "price_raw": "150.000 €",
            "location_raw": "Almada",
            "url": "https://olx.pt/d/t2-ID123.html",
            "description": "Particular, sem intermediários.",
        }
        result = self.normalizer.normalize("olx", raw)
        assert result is not None
        assert result["is_owner"] is True

    def test_agency_detection(self):
        raw = {
            "title": "T2 Lisboa",
            "price_raw": "250.000 €",
            "location_raw": "Lisboa",
            "url": "https://olx.pt/d/t2-ID456.html",
            "agency_name": "Remax Lisboa",
            "description": "Imobiliária Remax apresenta este excelente T2.",
        }
        result = self.normalizer.normalize("olx", raw)
        assert result is not None
        assert result["is_owner"] is False

    def test_condition_detection_renovar(self):
        raw = {
            "title": "T3 precisa de obras",
            "price_raw": "120.000 €",
            "location_raw": "Seixal",
            "url": "https://olx.pt/d/t3-ID789.html",
            "description": "Para renovar, precisa obras.",
        }
        result = self.normalizer.normalize("olx", raw)
        assert result is not None
        assert result["condition"] == "renovar"


class TestDeduplicator:
    deduplicator = Deduplicator()

    def test_same_inputs_same_fingerprint(self):
        data = {"typology": "T2", "zone": "Lisboa", "price": 200_000, "area_m2": 80, "title": "T2 Lisboa Alvalade"}
        fp1 = self.deduplicator.compute_fingerprint(data)
        fp2 = self.deduplicator.compute_fingerprint(data)
        assert fp1 == fp2

    def test_small_price_difference_same_fingerprint(self):
        data1 = {"typology": "T2", "zone": "Lisboa", "price": 200_000, "area_m2": 80, "title": "T2 Lisboa"}
        data2 = {"typology": "T2", "zone": "Lisboa", "price": 200_500, "area_m2": 80, "title": "T2 Lisboa"}
        fp1 = self.deduplicator.compute_fingerprint(data1)
        fp2 = self.deduplicator.compute_fingerprint(data2)
        assert fp1 == fp2  # rounded to nearest 1k

    def test_different_zone_different_fingerprint(self):
        data1 = {"typology": "T2", "zone": "Lisboa", "price": 200_000, "area_m2": 80, "title": "T2 Lisboa"}
        data2 = {"typology": "T2", "zone": "Cascais", "price": 200_000, "area_m2": 80, "title": "T2 Cascais"}
        fp1 = self.deduplicator.compute_fingerprint(data1)
        fp2 = self.deduplicator.compute_fingerprint(data2)
        assert fp1 != fp2

    def test_price_change_detection(self):
        assert self.deduplicator.detect_price_change(200_000, 190_000) is True   # 5% drop
        assert self.deduplicator.detect_price_change(200_000, 200_100) is False  # < 0.5%
        assert self.deduplicator.detect_price_change(None, 200_000) is False

    def test_merge_sources(self):
        existing = [{"source": "olx", "url": "http://olx.pt/1", "seen_at": "2024-01-01"}]
        merged = self.deduplicator.merge_sources(existing, "idealista", "http://idealista.pt/1")
        assert len(merged) == 2
        assert merged[1]["source"] == "idealista"

    def test_merge_sources_no_duplicate(self):
        existing = [{"source": "olx", "url": "http://olx.pt/1", "seen_at": "2024-01-01"}]
        merged = self.deduplicator.merge_sources(existing, "olx", "http://olx.pt/1")
        assert len(merged) == 1


class TestEnricher:
    enricher = Enricher()

    def test_price_per_m2_computed(self):
        data = {"price": 200_000, "area_m2": 80, "zone": "Lisboa", "typology": "T2", "description": ""}
        result = self.enricher.enrich(data)
        assert result["price_per_m2"] == 2_500.0

    def test_price_delta_computed(self):
        data = {"price": 200_000, "area_m2": 80, "zone": "Lisboa", "typology": "T2", "description": ""}
        result = self.enricher.enrich(data)
        # Lisboa T2 benchmark = 4500 €/m²; actual = 2500 €/m²; delta ≈ 44.4%
        assert result["price_delta_pct"] > 0

    def test_urgency_detected(self):
        data = {
            "price": 200_000, "area_m2": 80,
            "zone": "Lisboa", "typology": "T2",
            "description": "Vendo urgente, preciso sair do país.",
            "title": "",
        }
        result = self.enricher.enrich(data)
        assert result["_urgency_score"] > 0

    def test_get_zone_stats_returns_dict(self):
        stats = self.enricher.get_zone_stats()
        assert "Lisboa" in stats
        assert "Cascais" in stats
        assert "T2" in stats["Lisboa"]
