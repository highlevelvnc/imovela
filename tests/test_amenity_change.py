"""Tests for amenity_tags + change_detector + csv_importer."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from utils.amenity_tags import extract_amenities, format_tags, parse_tags
from pipeline.change_detector import _classify_text, _similarity, ChangeDetector
from pipeline.csv_importer import _parse_price, _parse_area, _parse_bool, _build_column_map


class TestAmenityTags:
    def test_pool_garage_view(self):
        out = extract_amenities("Apartamento T2 com piscina, garagem e vista para o mar")
        assert "piscina" in out
        assert "garagem" in out
        assert "vista_mar" in out

    def test_renovation(self):
        out = extract_amenities("T3 totalmente remodelado")
        assert "remodelado" in out

    def test_no_match(self):
        assert extract_amenities("Sem nada importante") == []

    def test_format_roundtrip(self):
        # format_tags sorts alphabetically — assertion mirrors that.
        tags = ["piscina", "garagem"]
        s = format_tags(tags)
        assert parse_tags(s) == sorted(tags)


class TestChangeDetector:
    def test_rewrite(self):
        v = _classify_text(
            "Apartamento T2 zona Lisboa centro perto metro com varanda excelente",
            "OPORTUNIDADE UNICA preço a discutir T2 imperdivel ligar ja!",
        )
        assert v == "rewrite"

    def test_edit_minor(self):
        v = _classify_text(
            "T2 Lisboa centro perto metro com varanda excelente vista",
            "T2 Lisboa centro perto do metro com varanda excelente vista",
        )
        assert v == "edit"

    def test_no_change(self):
        assert _classify_text("same text", "same text") is None

    def test_first_fill_not_change(self):
        assert _classify_text("", "new text") is None


class TestCSVImporter:
    def test_pt_thousand_separator(self):
        assert _parse_price("250.000 EUR") == 250000.0

    def test_pt_decimal_comma(self):
        assert _parse_price("250,50") == 250.50

    def test_mixed_format(self):
        assert _parse_price("250.000,75") == 250000.75

    def test_area(self):
        assert _parse_area("85 m²") == 85.0

    def test_bool_pt(self):
        assert _parse_bool("Sim") is True
        assert _parse_bool("Não") is False
        assert _parse_bool("particular") is True

    def test_column_map(self):
        m = _build_column_map(["Nome", "Telefone", "Preço", "Zona", "Notas"])
        assert m["Nome"] == "contact_name"
        assert m["Telefone"] == "contact_phone"
        assert m["Preço"] == "price"
        assert m["Notas"] == "description"


class TestSimilarity:
    """Smoke test — index build requires DB so just ensure import works."""
    def test_import(self):
        from utils.similarity import similar_to, invalidate
        assert callable(similar_to)
        assert callable(invalidate)
