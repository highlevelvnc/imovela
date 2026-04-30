"""Tests for utils.phone + utils.phone_discovery + utils.email_extractor."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest
from utils.phone import (
    validate_pt_phone, best_phone, classify_phone_type,
    extract_phone_from_text, extract_phone_from_tel_href, extract_whatsapp,
)
from utils.phone_discovery import discover_phones, discover_whatsapp, best_real_phone
from utils.email_extractor import extract_first_email, is_generic_portal_email


class TestPhoneValidation:
    def test_canonical_mobile(self):
        r = validate_pt_phone("+351912345678")
        assert r.valid and r.phone_type == "mobile" and r.confidence == 90

    def test_canonical_relay(self):
        r = validate_pt_phone("+351668711891")
        assert r.valid and r.phone_type == "relay"

    def test_six_repeat_rejected(self):
        assert not validate_pt_phone("+351999999999").valid

    def test_landline(self):
        assert validate_pt_phone("+351211234567").phone_type == "landline"

    def test_premium_rejected(self):
        assert not validate_pt_phone("+351707123456").valid


class TestBestPhone:
    def test_mobile_beats_landline_beats_relay(self):
        b = best_phone(["+351668711891", "+351912345678", "+351211234567"])
        assert b.canonical == "+351912345678"

    def test_only_relay(self):
        b = best_phone(["+351668711891"])
        assert b.canonical == "+351668711891"

    def test_empty_list(self):
        assert best_phone([]) is None


class TestPhoneDiscovery:
    def test_real_beats_relay_in_html(self):
        html = """
        <a href="tel:+351668711891">Mostrar número</a>
        <script>window.__seller = {phone: "+351912345678"};</script>
        """
        non_relay = discover_phones(html, allow_relay=False)
        assert "+351912345678" in non_relay
        assert "+351668711891" not in non_relay

    def test_whatsapp_link(self):
        html = '<a href="https://wa.me/351912345678">WhatsApp</a>'
        assert "+351912345678" in discover_whatsapp(html)

    def test_data_attr(self):
        html = '<div data-phone="912345678"></div>'
        assert "+351912345678" in discover_phones(html, allow_relay=False)

    def test_microdata(self):
        html = '<span itemprop="telephone">+351 912 345 678</span>'
        assert "+351912345678" in discover_phones(html, allow_relay=False)

    def test_best_real_phone_skips_relay(self):
        html = "Tel: +351 668 711 891 ou +351 912 345 678"
        assert best_real_phone(html) == "+351912345678"


class TestEmailExtraction:
    def test_mailto_href(self):
        assert extract_first_email('<a href="mailto:joao@gmail.com">') == "joao@gmail.com"

    def test_obfuscation_pt(self):
        assert extract_first_email("pedro arroba sapo ponto pt") == "pedro@sapo.pt"

    def test_sentry_filtered(self):
        assert extract_first_email(
            "feffe528c390ea66992a4a05131c3c68@o1338151.ingest.sentry.io"
        ) == ""

    def test_generic_local_rejected(self):
        assert is_generic_portal_email("noreply@gmail.com")
        assert is_generic_portal_email("info@imovirtual.com")

    def test_real_email_kept(self):
        assert not is_generic_portal_email("joao.silva@gmail.com")

    def test_lorem_rejected(self):
        assert extract_first_email("lectus@nisl.donec") == ""

    def test_version_string_rejected(self):
        assert extract_first_email("owl.carousel@2.3.4") == ""
