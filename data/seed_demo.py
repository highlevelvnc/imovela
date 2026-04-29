"""
Demo seed script — populates the database with realistic Portuguese real estate leads
for client demo / testing purposes.

Run via:  python main.py seed-demo
Direct:   python data/seed_demo.py
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fp(*parts: str) -> str:
    """Generate a deterministic fingerprint from parts."""
    raw = "|".join(str(p) for p in parts)
    return hashlib.sha256(raw.encode()).hexdigest()


def _ago(days: int) -> datetime:
    return datetime.utcnow() - timedelta(days=days)


# ─── Demo dataset ─────────────────────────────────────────────────────────────
# 35 leads: Lisboa(10), Cascais(8), Sintra(5), Almada(4), Seixal(4), Sesimbra(4)
# Labels: HOT(6), WARM(16), COLD(13)
# CRM stages distributed across all stages

LEADS = [
    # ── LISBOA — 10 leads ──────────────────────────────────────────────────────
    {
        "title": "T2 renovado com terraço — Mouraria, Lisboa Centro",
        "typology": "T2", "zone": "Lisboa", "area_m2": 78.0,
        "price": 285000.0, "price_per_m2": 3654.0, "price_benchmark": 4500.0,
        "price_delta_pct": 18.8,
        "condition": "Renovado",
        "address": "Rua da Mouraria 34, Lisboa",
        "contact_name": "João Fonseca", "contact_phone": "912 345 678",
        "is_owner": True, "agency_name": None,
        "days_on_market": 8, "price_changes": 0,
        "score": 88, "score_label": "HOT",
        "score_breakdown": {"price_opportunity": 28, "urgency_signals": 22, "owner_direct": 20, "days_on_market": 10, "data_quality": 4, "zone_priority": 4},
        "crm_stage": "negociação", "priority_flag": True,
        "description": "Apartamento T2 completamente renovado em 2023. Terraço privativo de 12m². Proprietário a emigrar com urgência — preço negociável. Cozinha equipada, AC, vidros duplos. Excelente localização a 5min do Rossio.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t2-mouraria-lisboa-demo-001"}],
        "first_seen_at": _ago(8), "notes": [
            {"note": "Proprietário confirmou urgência de venda por emigração. Proposta de 270k aceite em princípio.", "note_type": "call"},
            {"note": "Visita marcada para amanhã às 10h.", "note_type": "internal"},
        ],
    },
    {
        "title": "T3 com garagem — Benfica, oportunidade herança",
        "typology": "T3", "zone": "Lisboa", "area_m2": 105.0,
        "price": 340000.0, "price_per_m2": 3238.0, "price_benchmark": 4200.0,
        "price_delta_pct": 22.9,
        "condition": "Para remodelar",
        "address": "Rua Direita de Benfica 211, Lisboa",
        "contact_name": "Maria Correia", "contact_phone": "965 432 109",
        "is_owner": True, "agency_name": None,
        "days_on_market": 45, "price_changes": 1,
        "score": 81, "score_label": "HOT",
        "score_breakdown": {"price_opportunity": 27, "urgency_signals": 20, "owner_direct": 20, "days_on_market": 8, "data_quality": 3, "zone_priority": 3},
        "crm_stage": "contactado", "priority_flag": True,
        "description": "Imóvel de herança, herdeiros querem venda rápida. T3 com garagem, arrecadação. Precisa remodelação mas estrutura excelente. Último andar com vista. Descida de preço de 20k há 2 semanas.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/lisboa/oferta/demo-002"}],
        "first_seen_at": _ago(45), "notes": [
            {"note": "Contacto estabelecido com herdeira Maria. Dispostos a negociar até 320k.", "note_type": "call"},
        ],
        "price_history": [360000.0],
    },
    {
        "title": "T1 moderno no Intendente — rendimento garantido",
        "typology": "T1", "zone": "Lisboa", "area_m2": 52.0,
        "price": 210000.0, "price_per_m2": 4038.0, "price_benchmark": 4500.0,
        "price_delta_pct": 10.3,
        "condition": "Bom estado",
        "address": "Largo do Intendente 8, Lisboa",
        "contact_name": "Hugo Martins", "contact_phone": "916 789 012",
        "is_owner": False, "agency_name": "Remax Lisboa",
        "days_on_market": 12, "price_changes": 0,
        "score": 72, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 14, "urgency_signals": 15, "owner_direct": 0, "days_on_market": 10, "data_quality": 5, "zone_priority": 5},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T1 totalmente equipado, pronto a arrendar. Renda actual de 950€/mês — yield de 5.4%. Zona em forte valorização. Aquecimento central, AC, arrecadação.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t1-intendente-demo-003"}],
        "first_seen_at": _ago(12), "notes": [],
    },
    {
        "title": "T2+1 em Alvalade — proprietário a emigrar",
        "typology": "T2", "zone": "Lisboa", "area_m2": 90.0,
        "price": 395000.0, "price_per_m2": 4389.0, "price_benchmark": 4500.0,
        "price_delta_pct": 2.5,
        "condition": "Renovado",
        "address": "Av. Rio de Janeiro 56, Alvalade, Lisboa",
        "contact_name": "Ana Ribeiro", "contact_phone": "931 234 567",
        "is_owner": True, "agency_name": None,
        "days_on_market": 3, "price_changes": 0,
        "score": 69, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 8, "urgency_signals": 22, "owner_direct": 20, "days_on_market": 12, "data_quality": 4, "zone_priority": 3},
        "crm_stage": "novo", "priority_flag": False,
        "description": "Proprietária parte para Dubai em 6 semanas. Precisa fechar negócio rapidamente. T2+1 renovado em 2022, nova cozinha, 2 casas de banho. Piso 4 sem elevador.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/lisboa/oferta/demo-004"}],
        "first_seen_at": _ago(3), "notes": [],
    },
    {
        "title": "T4 moradia em Carnide com jardim",
        "typology": "T4", "zone": "Lisboa", "area_m2": 180.0,
        "price": 650000.0, "price_per_m2": 3611.0, "price_benchmark": 4200.0,
        "price_delta_pct": 14.0,
        "condition": "Bom estado",
        "address": "Rua dos Poios 5, Carnide, Lisboa",
        "contact_name": "Pedro Neves", "contact_phone": "961 321 654",
        "is_owner": True, "agency_name": None,
        "days_on_market": 62, "price_changes": 2,
        "score": 77, "score_label": "HOT",
        "score_breakdown": {"price_opportunity": 20, "urgency_signals": 12, "owner_direct": 20, "days_on_market": 15, "data_quality": 5, "zone_priority": 5},
        "crm_stage": "contactado", "priority_flag": True,
        "description": "Moradia unifamiliar com jardim de 200m² e piscina. 4 quartos, 3 casas de banho, garagem dupla. Proprietário reformado, 2 reduções de preço nos últimos 3 meses.",
        "sources": [
            {"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t4-carnide-demo-005"},
            {"source": "idealista", "url": "https://www.idealista.pt/imovel/demo-005"},
        ],
        "first_seen_at": _ago(62), "notes": [
            {"note": "Proprietário receptivo. Diz que aceita 620k. Marcar visita esta semana.", "note_type": "call"},
        ],
        "price_history": [720000.0, 680000.0],
    },
    {
        "title": "T0 estúdio renovado — Bairro Alto",
        "typology": "T0", "zone": "Lisboa", "area_m2": 35.0,
        "price": 175000.0, "price_per_m2": 5000.0, "price_benchmark": 5000.0,
        "price_delta_pct": 0.0,
        "condition": "Renovado",
        "address": "Travessa da Queimada 12, Bairro Alto, Lisboa",
        "contact_name": None, "contact_phone": None,
        "is_owner": False, "agency_name": "ERA Lisboa",
        "days_on_market": 5, "price_changes": 0,
        "score": 38, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 2, "urgency_signals": 5, "owner_direct": 0, "days_on_market": 12, "data_quality": 2, "zone_priority": 5},
        "crm_stage": "novo", "priority_flag": False,
        "description": "Estúdio no coração do Bairro Alto. Renovado em 2024. Kitchenette equipada. Ideal para investimento turístico — AL já activo.",
        "sources": [{"source": "idealista", "url": "https://www.idealista.pt/imovel/demo-006"}],
        "first_seen_at": _ago(5), "notes": [],
    },
    {
        "title": "T2 em Arroios — 90 dias sem vender",
        "typology": "T2", "zone": "Lisboa", "area_m2": 72.0,
        "price": 295000.0, "price_per_m2": 4097.0, "price_benchmark": 4500.0,
        "price_delta_pct": 8.9,
        "condition": "Bom estado",
        "address": "Rua Maria Andrade 45, Arroios, Lisboa",
        "contact_name": "Carlos Mendes", "contact_phone": "912 111 222",
        "is_owner": False, "agency_name": "Imoconsult",
        "days_on_market": 93, "price_changes": 1,
        "score": 55, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 13, "urgency_signals": 8, "owner_direct": 0, "days_on_market": 15, "data_quality": 5, "zone_priority": 4},
        "crm_stage": "novo", "priority_flag": False,
        "description": "Apartamento luminoso com varanda. Cozinha equipada. 93 dias no mercado, já com redução de preço. Vendedor motivado.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t2-arroios-demo-007"}],
        "first_seen_at": _ago(93), "notes": [],
        "price_history": [315000.0],
    },
    {
        "title": "T3 duplex na Penha de França",
        "typology": "T3", "zone": "Lisboa", "area_m2": 125.0,
        "price": 480000.0, "price_per_m2": 3840.0, "price_benchmark": 4200.0,
        "price_delta_pct": 8.6,
        "condition": "Bom estado",
        "address": "Rua Morais Soares 110, Penha de França, Lisboa",
        "contact_name": "Sofia Lopes", "contact_phone": "962 444 555",
        "is_owner": True, "agency_name": None,
        "days_on_market": 28, "price_changes": 0,
        "score": 60, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 14, "urgency_signals": 8, "owner_direct": 20, "days_on_market": 8, "data_quality": 5, "zone_priority": 5},
        "crm_stage": "ganho", "priority_flag": False,
        "description": "Duplex espaçoso com 3 quartos, 2 WC, varanda com vista sobre Lisboa. Proprietária directa, muito receptiva. Edifício com elevador.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/lisboa/oferta/demo-008"}],
        "first_seen_at": _ago(28), "notes": [
            {"note": "Negócio fechado a 465k. Escritura marcada para 15/04.", "note_type": "internal"},
        ],
    },
    {
        "title": "T1 no Príncipe Real — excelente rendimento",
        "typology": "T1", "zone": "Lisboa", "area_m2": 48.0,
        "price": 320000.0, "price_per_m2": 6667.0, "price_benchmark": 5000.0,
        "price_delta_pct": -33.3,
        "condition": "Excelente",
        "address": "Rua Dom Pedro V 78, Príncipe Real, Lisboa",
        "contact_name": None, "contact_phone": None,
        "is_owner": False, "agency_name": "Savills Portugal",
        "days_on_market": 10, "price_changes": 0,
        "score": 28, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 0, "urgency_signals": 3, "owner_direct": 0, "days_on_market": 10, "data_quality": 2, "zone_priority": 5},
        "crm_stage": "novo", "priority_flag": False,
        "description": "Apartamento premium no Príncipe Real. Totalmente mobilado e equipado. Actualmente em AL com ocupação de 85%.",
        "sources": [{"source": "idealista", "url": "https://www.idealista.pt/imovel/demo-009"}],
        "first_seen_at": _ago(10), "notes": [],
    },
    {
        "title": "T2 na Ajuda — processo de divórcio, venda urgente",
        "typology": "T2", "zone": "Lisboa", "area_m2": 82.0,
        "price": 255000.0, "price_per_m2": 3110.0, "price_benchmark": 4200.0,
        "price_delta_pct": 26.0,
        "condition": "Usado",
        "address": "Calçada da Ajuda 23, Lisboa",
        "contact_name": "Rui Azevedo", "contact_phone": "913 666 777",
        "is_owner": True, "agency_name": None,
        "days_on_market": 15, "price_changes": 0,
        "score": 85, "score_label": "HOT",
        "score_breakdown": {"price_opportunity": 28, "urgency_signals": 25, "owner_direct": 20, "days_on_market": 8, "data_quality": 4, "zone_priority": 0},
        "crm_stage": "negociação", "priority_flag": True,
        "description": "Venda urgente por divórcio. Proprietário quer fechar em 30 dias. T2 em bom estado, cozinha renovada, varanda. Preço muito abaixo do mercado para fechar rápido.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/lisboa/oferta/demo-010"}],
        "first_seen_at": _ago(15), "notes": [
            {"note": "Urgência confirmada. Aceita 245k para fechar até fim do mês.", "note_type": "call"},
            {"note": "Proposta enviada a 245k. À espera de resposta.", "note_type": "email"},
        ],
    },

    # ── CASCAIS — 8 leads ──────────────────────────────────────────────────────
    {
        "title": "T3 Cascais Centro, 100m da praia",
        "typology": "T3", "zone": "Cascais", "area_m2": 130.0,
        "price": 520000.0, "price_per_m2": 4000.0, "price_benchmark": 4700.0,
        "price_delta_pct": 14.9,
        "condition": "Renovado",
        "address": "Rua Frederico Arouca 55, Cascais",
        "contact_name": "Marta Silva", "contact_phone": "966 888 111",
        "is_owner": True, "agency_name": None,
        "days_on_market": 5, "price_changes": 0,
        "score": 78, "score_label": "HOT",
        "score_breakdown": {"price_opportunity": 22, "urgency_signals": 15, "owner_direct": 20, "days_on_market": 12, "data_quality": 4, "zone_priority": 5},
        "crm_stage": "contactado", "priority_flag": True,
        "description": "T3 renovado em 2023, a 100m da praia de Cascais. Proprietária directa, família a mudar para Porto. Preço negociável. Terraço com vista mar parcial, garagem.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t3-cascais-demo-011"}],
        "first_seen_at": _ago(5), "notes": [
            {"note": "Primeira chamada positiva. Proposta de visita aceite para sexta.", "note_type": "call"},
        ],
    },
    {
        "title": "Moradia T4+1 em São João do Estoril",
        "typology": "T4", "zone": "Cascais", "area_m2": 220.0,
        "price": 890000.0, "price_per_m2": 4045.0, "price_benchmark": 4700.0,
        "price_delta_pct": 13.9,
        "condition": "Excelente",
        "address": "Avenida de Sabóia 180, São João do Estoril",
        "contact_name": "Francisco Costa", "contact_phone": "961 999 000",
        "is_owner": True, "agency_name": None,
        "days_on_market": 40, "price_changes": 1,
        "score": 74, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 21, "urgency_signals": 8, "owner_direct": 20, "days_on_market": 10, "data_quality": 5, "zone_priority": 5},
        "crm_stage": "negociação", "priority_flag": False,
        "description": "Moradia de luxo com piscina, jardim e vista mar. 5 quartos, 4 WC. Proprietário executivo expatriado. Redução de 50k há 3 semanas. Garagem 3 carros.",
        "sources": [
            {"source": "idealista", "url": "https://www.idealista.pt/imovel/demo-012"},
            {"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t4-estoril-demo-012"},
        ],
        "first_seen_at": _ago(40), "notes": [
            {"note": "Contador-proposta enviada: 840k. Francisco está a considerar.", "note_type": "email"},
        ],
        "price_history": [940000.0],
    },
    {
        "title": "T2 com vista mar em Estoril",
        "typology": "T2", "zone": "Cascais", "area_m2": 85.0,
        "price": 420000.0, "price_per_m2": 4941.0, "price_benchmark": 4700.0,
        "price_delta_pct": -5.1,
        "condition": "Bom estado",
        "address": "Av. de Nice 23, Estoril",
        "contact_name": None, "contact_phone": None,
        "is_owner": False, "agency_name": "JLL Portugal",
        "days_on_market": 18, "price_changes": 0,
        "score": 32, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 0, "urgency_signals": 5, "owner_direct": 0, "days_on_market": 10, "data_quality": 2, "zone_priority": 5},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T2 com varanda e vista mar frontal. Condomínio privado com piscina. Preço ligeiramente acima de mercado.",
        "sources": [{"source": "idealista", "url": "https://www.idealista.pt/imovel/demo-013"}],
        "first_seen_at": _ago(18), "notes": [],
    },
    {
        "title": "T1 em Cascais Vila — ideal investimento",
        "typology": "T1", "zone": "Cascais", "area_m2": 55.0,
        "price": 235000.0, "price_per_m2": 4273.0, "price_benchmark": 4700.0,
        "price_delta_pct": 9.1,
        "condition": "Renovado",
        "address": "Rua da Misericórdia 7, Cascais",
        "contact_name": "Luísa Tavares", "contact_phone": "912 555 888",
        "is_owner": False, "agency_name": "Century 21 Cascais",
        "days_on_market": 22, "price_changes": 0,
        "score": 51, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 14, "urgency_signals": 5, "owner_direct": 0, "days_on_market": 8, "data_quality": 5, "zone_priority": 5},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T1 renovado no centro histórico de Cascais. A 200m da praia. Rentabilidade estimada 4.8% ano. Condomínio baixo.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t1-cascais-demo-014"}],
        "first_seen_at": _ago(22), "notes": [],
    },
    {
        "title": "T3 em Birre — herança, venda rápida",
        "typology": "T3", "zone": "Cascais", "area_m2": 110.0,
        "price": 410000.0, "price_per_m2": 3727.0, "price_benchmark": 4700.0,
        "price_delta_pct": 20.7,
        "condition": "Para remodelar",
        "address": "Rua das Flores 34, Birre, Cascais",
        "contact_name": "Ricardo Branco", "contact_phone": "963 777 444",
        "is_owner": True, "agency_name": None,
        "days_on_market": 12, "price_changes": 0,
        "score": 79, "score_label": "HOT",
        "score_breakdown": {"price_opportunity": 26, "urgency_signals": 20, "owner_direct": 20, "days_on_market": 8, "data_quality": 5, "zone_priority": 0},
        "crm_stage": "contactado", "priority_flag": True,
        "description": "Imóvel de herança com 3 filhos herdeiros todos de acordo em vender. T3 precisa remodelação completa mas estrutura sólida. Zona premium de Birre — oportunidade clara.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/cascais/oferta/demo-015"}],
        "first_seen_at": _ago(12), "notes": [
            {"note": "Herdeiro Ricardo contactado. Preferem proposta limpa sem condições. Máximo 2 semanas para fechar.", "note_type": "call"},
        ],
    },
    {
        "title": "T2 Alcabideche — tranquilo e barato",
        "typology": "T2", "zone": "Cascais", "area_m2": 80.0,
        "price": 280000.0, "price_per_m2": 3500.0, "price_benchmark": 4700.0,
        "price_delta_pct": 25.5,
        "condition": "Usado",
        "address": "Rua da Liberdade 120, Alcabideche",
        "contact_name": "Teresa Campos", "contact_phone": "912 333 999",
        "is_owner": True, "agency_name": None,
        "days_on_market": 75, "price_changes": 2,
        "score": 70, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 25, "urgency_signals": 5, "owner_direct": 20, "days_on_market": 15, "data_quality": 5, "zone_priority": 0},
        "crm_stage": "perdido", "priority_flag": False,
        "description": "T2 usado mas bem conservado. Zona calma. 2 reduções de preço. Proprietária acabou por fechar com outro comprador.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t2-alcabideche-demo-016"}],
        "first_seen_at": _ago(75), "notes": [
            {"note": "Imóvel vendido a outro interessante. Contacto perdido.", "note_type": "internal"},
        ],
        "price_history": [310000.0, 295000.0],
    },
    {
        "title": "T4 moradia em Monte Estoril",
        "typology": "T4", "zone": "Cascais", "area_m2": 195.0,
        "price": 780000.0, "price_per_m2": 4000.0, "price_benchmark": 4700.0,
        "price_delta_pct": 14.9,
        "condition": "Bom estado",
        "address": "Av. do Parque 88, Monte Estoril",
        "contact_name": None, "contact_phone": None,
        "is_owner": False, "agency_name": "Engel & Völkers",
        "days_on_market": 30, "price_changes": 0,
        "score": 40, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 22, "urgency_signals": 3, "owner_direct": 0, "days_on_market": 8, "data_quality": 2, "zone_priority": 5},
        "crm_stage": "novo", "priority_flag": False,
        "description": "Moradia com jardim e piscina. 4 quartos en-suite. Agência premium sem informação de contacto directa. Preço negociável.",
        "sources": [{"source": "idealista", "url": "https://www.idealista.pt/imovel/demo-017"}],
        "first_seen_at": _ago(30), "notes": [],
    },
    {
        "title": "T2 renovado em Cascais — excelente localização",
        "typology": "T2", "zone": "Cascais", "area_m2": 75.0,
        "price": 365000.0, "price_per_m2": 4867.0, "price_benchmark": 4700.0,
        "price_delta_pct": -3.5,
        "condition": "Renovado",
        "address": "Rua da Palmeira 17, Cascais",
        "contact_name": "Nuno Sousa", "contact_phone": "965 111 333",
        "is_owner": False, "agency_name": "Remax Cascais",
        "days_on_market": 7, "price_changes": 0,
        "score": 42, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 0, "urgency_signals": 8, "owner_direct": 0, "days_on_market": 12, "data_quality": 5, "zone_priority": 5},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T2 renovado, mobilado e equipado. A 5 min a pé da praia e do centro. Preço ligeiramente acima do benchmark.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t2-cascais-centro-demo-018"}],
        "first_seen_at": _ago(7), "notes": [],
    },

    # ── SINTRA — 5 leads ────────────────────────────────────────────────────────
    {
        "title": "T3 em Mem Martins — vivendas em lote próprio",
        "typology": "T3", "zone": "Sintra", "area_m2": 120.0,
        "price": 280000.0, "price_per_m2": 2333.0, "price_benchmark": 2700.0,
        "price_delta_pct": 13.6,
        "condition": "Bom estado",
        "address": "Rua do Castelo 89, Mem Martins, Sintra",
        "contact_name": "Joaquim Ferreira", "contact_phone": "912 777 666",
        "is_owner": True, "agency_name": None,
        "days_on_market": 55, "price_changes": 1,
        "score": 62, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 20, "urgency_signals": 8, "owner_direct": 20, "days_on_market": 10, "data_quality": 4, "zone_priority": 0},
        "crm_stage": "contactado", "priority_flag": False,
        "description": "Moradia geminada com lote de 200m². 3 quartos, 2 WC, garagem. Proprietário a vender para ir viver perto da família. Preço com margem de negociação.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/sintra/oferta/demo-019"}],
        "first_seen_at": _ago(55), "notes": [
            {"note": "Visita realizada. Casa em bom estado. Proprietário aceita 265k.", "note_type": "visit"},
        ],
        "price_history": [295000.0],
    },
    {
        "title": "T4 Sintra histórica — herança partilhada",
        "typology": "T4", "zone": "Sintra", "area_m2": 160.0,
        "price": 380000.0, "price_per_m2": 2375.0, "price_benchmark": 2700.0,
        "price_delta_pct": 12.0,
        "condition": "Para remodelar",
        "address": "Rua Visconde de Monserrate 12, Sintra",
        "contact_name": "Graça Pereira", "contact_phone": "913 888 555",
        "is_owner": True, "agency_name": None,
        "days_on_market": 30, "price_changes": 0,
        "score": 65, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 18, "urgency_signals": 20, "owner_direct": 20, "days_on_market": 8, "data_quality": 4, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "Casa de herança em Sintra histórica. Quatro irmãos querem vender com urgência. Precisa obra mas bones estruturais excelentes. Vista para o Palácio.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t4-sintra-demo-020"}],
        "first_seen_at": _ago(30), "notes": [],
    },
    {
        "title": "T2 em Agualva — pronto a entrar",
        "typology": "T2", "zone": "Sintra", "area_m2": 78.0,
        "price": 195000.0, "price_per_m2": 2500.0, "price_benchmark": 2700.0,
        "price_delta_pct": 7.4,
        "condition": "Bom estado",
        "address": "Rua Central 45, Agualva, Sintra",
        "contact_name": None, "contact_phone": None,
        "is_owner": False, "agency_name": "ERA Sintra",
        "days_on_market": 20, "price_changes": 0,
        "score": 35, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 11, "urgency_signals": 3, "owner_direct": 0, "days_on_market": 8, "data_quality": 3, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T2 em bom estado. Proximidade ao IC19 e linha Sintra. Sem contacto de proprietário.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t2-agualva-demo-021"}],
        "first_seen_at": _ago(20), "notes": [],
    },
    {
        "title": "T1 em Queluz — perto metro",
        "typology": "T1", "zone": "Sintra", "area_m2": 50.0,
        "price": 155000.0, "price_per_m2": 3100.0, "price_benchmark": 2700.0,
        "price_delta_pct": -14.8,
        "condition": "Usado",
        "address": "Av. António Enes 5, Queluz",
        "contact_name": None, "contact_phone": None,
        "is_owner": False, "agency_name": "Mediasol",
        "days_on_market": 60, "price_changes": 0,
        "score": 22, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 0, "urgency_signals": 3, "owner_direct": 0, "days_on_market": 10, "data_quality": 2, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T1 usado perto do metro de Queluz. Preço acima do benchmark local. Sem dados de contacto.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/sintra/oferta/demo-022"}],
        "first_seen_at": _ago(60), "notes": [],
    },
    {
        "title": "T3 em Rio de Mouro — espaçoso com garagem",
        "typology": "T3", "zone": "Sintra", "area_m2": 115.0,
        "price": 265000.0, "price_per_m2": 2304.0, "price_benchmark": 2700.0,
        "price_delta_pct": 14.7,
        "condition": "Bom estado",
        "address": "Rua Pinhal de Frades 23, Rio de Mouro",
        "contact_name": "Manuel Santos", "contact_phone": "964 222 888",
        "is_owner": True, "agency_name": None,
        "days_on_market": 35, "price_changes": 0,
        "score": 58, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 22, "urgency_signals": 5, "owner_direct": 20, "days_on_market": 8, "data_quality": 3, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T3 amplo com garagem. Proprietário a fazer downsize após filhos saírem de casa. Zona tranquila com bons acessos à A9.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t3-riodemouro-demo-023"}],
        "first_seen_at": _ago(35), "notes": [],
    },

    # ── ALMADA — 4 leads ────────────────────────────────────────────────────────
    {
        "title": "T2 na Costa da Caparica — vista mar",
        "typology": "T2", "zone": "Almada", "area_m2": 72.0,
        "price": 195000.0, "price_per_m2": 2708.0, "price_benchmark": 2500.0,
        "price_delta_pct": -8.3,
        "condition": "Usado",
        "address": "Rua dos Pescadores 44, Costa da Caparica",
        "contact_name": None, "contact_phone": None,
        "is_owner": False, "agency_name": "Remax Almada",
        "days_on_market": 25, "price_changes": 0,
        "score": 30, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 0, "urgency_signals": 3, "owner_direct": 0, "days_on_market": 8, "data_quality": 2, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T2 com vista mar parcial na Caparica. Preço acima do benchmark. Agência sem contacto de proprietário.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/almada/oferta/demo-024"}],
        "first_seen_at": _ago(25), "notes": [],
    },
    {
        "title": "T3 em Almada Centro — proprietário urgente",
        "typology": "T3", "zone": "Almada", "area_m2": 108.0,
        "price": 230000.0, "price_per_m2": 2130.0, "price_benchmark": 2500.0,
        "price_delta_pct": 14.8,
        "condition": "Bom estado",
        "address": "Rua Cândido dos Reis 67, Almada",
        "contact_name": "António Rodrigues", "contact_phone": "916 444 222",
        "is_owner": True, "agency_name": None,
        "days_on_market": 20, "price_changes": 0,
        "score": 65, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 22, "urgency_signals": 15, "owner_direct": 20, "days_on_market": 8, "data_quality": 0, "zone_priority": 0},
        "crm_stage": "contactado", "priority_flag": False,
        "description": "Proprietário a trabalhar nos Açores, não consegue gerir o imóvel. Quer venda rápida. T3 em bom estado, 3 quartos, 2 WC, garagem.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t3-almada-demo-025"}],
        "first_seen_at": _ago(20), "notes": [
            {"note": "António receptivo. Disponível para proposta directa. Sugere 220k.", "note_type": "call"},
        ],
    },
    {
        "title": "T4 moradia em Charneca da Caparica",
        "typology": "T4", "zone": "Almada", "area_m2": 165.0,
        "price": 350000.0, "price_per_m2": 2121.0, "price_benchmark": 2500.0,
        "price_delta_pct": 15.2,
        "condition": "Bom estado",
        "address": "Rua das Acácias 12, Charneca da Caparica",
        "contact_name": "Isabel Moura", "contact_phone": "962 123 456",
        "is_owner": True, "agency_name": None,
        "days_on_market": 80, "price_changes": 2,
        "score": 68, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 23, "urgency_signals": 5, "owner_direct": 20, "days_on_market": 15, "data_quality": 5, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "Moradia com piscina e jardim. 4 quartos, 3 WC, garagem dupla. 80 dias no mercado com 2 reduções. Proprietária muito motivada.",
        "sources": [{"source": "idealista", "url": "https://www.idealista.pt/imovel/demo-026"}],
        "first_seen_at": _ago(80), "notes": [],
        "price_history": [395000.0, 370000.0],
    },
    {
        "title": "T1 em Cacilhas — perto ferry",
        "typology": "T1", "zone": "Almada", "area_m2": 45.0,
        "price": 140000.0, "price_per_m2": 3111.0, "price_benchmark": 2500.0,
        "price_delta_pct": -24.4,
        "condition": "Para remodelar",
        "address": "Rua do Ginjal 5, Cacilhas",
        "contact_name": None, "contact_phone": None,
        "is_owner": False, "agency_name": "Imoconsult Almada",
        "days_on_market": 45, "price_changes": 0,
        "score": 18, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 0, "urgency_signals": 3, "owner_direct": 0, "days_on_market": 10, "data_quality": 2, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T1 para remodelação total em Cacilhas. Preço acima do mercado. Sem contacto directo.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/almada/oferta/demo-027"}],
        "first_seen_at": _ago(45), "notes": [],
    },

    # ── SEIXAL — 4 leads ────────────────────────────────────────────────────────
    {
        "title": "T3 em Corroios — excelente negócio",
        "typology": "T3", "zone": "Seixal", "area_m2": 102.0,
        "price": 180000.0, "price_per_m2": 1765.0, "price_benchmark": 2000.0,
        "price_delta_pct": 11.8,
        "condition": "Bom estado",
        "address": "Av. Afonso de Albuquerque 34, Corroios, Seixal",
        "contact_name": "Filipe Guerreiro", "contact_phone": "963 444 111",
        "is_owner": True, "agency_name": None,
        "days_on_market": 42, "price_changes": 1,
        "score": 60, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 17, "urgency_signals": 5, "owner_direct": 20, "days_on_market": 10, "data_quality": 5, "zone_priority": 0},
        "crm_stage": "contactado", "priority_flag": False,
        "description": "T3 espaçoso com garagem e arrecadação. Proprietário a fazer downsize. 1 redução de preço. Zona calma e familiar.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t3-corroios-demo-028"}],
        "first_seen_at": _ago(42), "notes": [
            {"note": "Visita agendada. Proprietário receptivo a 170k.", "note_type": "call"},
        ],
        "price_history": [195000.0],
    },
    {
        "title": "T2 no Seixal Centro — pronto a entrar",
        "typology": "T2", "zone": "Seixal", "area_m2": 75.0,
        "price": 155000.0, "price_per_m2": 2067.0, "price_benchmark": 2000.0,
        "price_delta_pct": -3.3,
        "condition": "Bom estado",
        "address": "Rua Álvaro Velho 22, Seixal",
        "contact_name": None, "contact_phone": None,
        "is_owner": False, "agency_name": "Era Seixal",
        "days_on_market": 15, "price_changes": 0,
        "score": 28, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 0, "urgency_signals": 3, "owner_direct": 0, "days_on_market": 10, "data_quality": 3, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T2 pronto a habitar no centro do Seixal. Sem diferencial de preço significativo.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/seixal/oferta/demo-029"}],
        "first_seen_at": _ago(15), "notes": [],
    },
    {
        "title": "T4 Fernão Ferro — moradia com lote grande",
        "typology": "T4", "zone": "Seixal", "area_m2": 150.0,
        "price": 260000.0, "price_per_m2": 1733.0, "price_benchmark": 2000.0,
        "price_delta_pct": 13.3,
        "condition": "Usado",
        "address": "Rua do Pinhal 56, Fernão Ferro",
        "contact_name": "Conceição Leal", "contact_phone": "912 000 333",
        "is_owner": True, "agency_name": None,
        "days_on_market": 65, "price_changes": 1,
        "score": 55, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 20, "urgency_signals": 5, "owner_direct": 20, "days_on_market": 10, "data_quality": 0, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "Moradia T4 com lote de 500m². Garagem e jardim. Proprietária de idade avançada, filha faz mediação. 1 redução de preço.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t4-fernaoforro-demo-030"}],
        "first_seen_at": _ago(65), "notes": [],
        "price_history": [280000.0],
    },
    {
        "title": "T1 em Amora — muito barato",
        "typology": "T1", "zone": "Seixal", "area_m2": 48.0,
        "price": 89000.0, "price_per_m2": 1854.0, "price_benchmark": 2000.0,
        "price_delta_pct": 7.3,
        "condition": "Para remodelar",
        "address": "Rua das Salinas 8, Amora, Seixal",
        "contact_name": "Pedro Carvalho", "contact_phone": "916 555 000",
        "is_owner": True, "agency_name": None,
        "days_on_market": 30, "price_changes": 0,
        "score": 47, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 11, "urgency_signals": 8, "owner_direct": 20, "days_on_market": 8, "data_quality": 0, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T1 para remodelação. Muito bom preço de entrada. Sem área confirmada, sem email.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/seixal/oferta/demo-031"}],
        "first_seen_at": _ago(30), "notes": [],
    },

    # ── SESIMBRA — 4 leads ──────────────────────────────────────────────────────
    {
        "title": "T2 em Sesimbra Vila — vista mar directa",
        "typology": "T2", "zone": "Sesimbra", "area_m2": 68.0,
        "price": 195000.0, "price_per_m2": 2868.0, "price_benchmark": 2400.0,
        "price_delta_pct": -19.5,
        "condition": "Bom estado",
        "address": "Rua Marques Pombal 12, Sesimbra",
        "contact_name": None, "contact_phone": None,
        "is_owner": False, "agency_name": "Local Immobilien",
        "days_on_market": 20, "price_changes": 0,
        "score": 20, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 0, "urgency_signals": 3, "owner_direct": 0, "days_on_market": 8, "data_quality": 2, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T2 com vista mar directa. Preço acima do benchmark local. Sem contacto de proprietário.",
        "sources": [{"source": "idealista", "url": "https://www.idealista.pt/imovel/demo-032"}],
        "first_seen_at": _ago(20), "notes": [],
    },
    {
        "title": "T3 em Santiago do Sesimbra — lote próprio",
        "typology": "T3", "zone": "Sesimbra", "area_m2": 110.0,
        "price": 235000.0, "price_per_m2": 2136.0, "price_benchmark": 2400.0,
        "price_delta_pct": 11.0,
        "condition": "Bom estado",
        "address": "Rua do Outeiro 45, Santiago do Sesimbra",
        "contact_name": "João Baptista", "contact_phone": "963 222 777",
        "is_owner": True, "agency_name": None,
        "days_on_market": 50, "price_changes": 1,
        "score": 54, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 17, "urgency_signals": 5, "owner_direct": 20, "days_on_market": 10, "data_quality": 2, "zone_priority": 0},
        "crm_stage": "contactado", "priority_flag": False,
        "description": "Moradia com lote de 300m². Proprietário a mudar para Setúbal. 1 redução de preço. Zona tranquila, bons acessos.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t3-sesimbra-demo-033"}],
        "first_seen_at": _ago(50), "notes": [
            {"note": "Proprietário João contactado. Aceita 225k. A ponderar visita.", "note_type": "call"},
        ],
        "price_history": [250000.0],
    },
    {
        "title": "T1 em Sesimbra — AL activo com boa yield",
        "typology": "T1", "zone": "Sesimbra", "area_m2": 44.0,
        "price": 148000.0, "price_per_m2": 3364.0, "price_benchmark": 2400.0,
        "price_delta_pct": -40.2,
        "condition": "Renovado",
        "address": "Rua dos Pescadores 8, Sesimbra",
        "contact_name": None, "contact_phone": None,
        "is_owner": False, "agency_name": "ERA Sesimbra",
        "days_on_market": 10, "price_changes": 0,
        "score": 15, "score_label": "COLD",
        "score_breakdown": {"price_opportunity": 0, "urgency_signals": 0, "owner_direct": 0, "days_on_market": 8, "data_quality": 2, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "T1 renovado com AL activo. Preço significativamente acima do mercado local.",
        "sources": [{"source": "olx", "url": "https://www.olx.pt/imoveis/apartamentos/venda/sesimbra/oferta/demo-034"}],
        "first_seen_at": _ago(10), "notes": [],
    },
    {
        "title": "T4 quinta em Sesimbra — terreno de 2ha",
        "typology": "T4", "zone": "Sesimbra", "area_m2": 200.0,
        "price": 450000.0, "price_per_m2": 2250.0, "price_benchmark": 2400.0,
        "price_delta_pct": 6.3,
        "condition": "Bom estado",
        "address": "Estrada de Azóia km 3, Sesimbra",
        "contact_name": "Rosa Fernandes", "contact_phone": "912 888 111",
        "is_owner": True, "agency_name": None,
        "days_on_market": 90, "price_changes": 2,
        "score": 52, "score_label": "WARM",
        "score_breakdown": {"price_opportunity": 10, "urgency_signals": 8, "owner_direct": 20, "days_on_market": 14, "data_quality": 0, "zone_priority": 0},
        "crm_stage": "novo", "priority_flag": False,
        "description": "Quinta com 2ha de terreno, piscina, casa principal T4 e dependências. 2 reduções de preço em 90 dias. Proprietária idosa, herdeiros a intermediar.",
        "sources": [{"source": "imovirtual", "url": "https://www.imovirtual.com/oferta/t4-quinta-sesimbra-demo-035"}],
        "first_seen_at": _ago(90), "notes": [],
        "price_history": [530000.0, 490000.0],
    },
]


def run_seed(clear_existing: bool = False) -> dict:
    """
    Populate database with demo leads.
    Returns {"created": int, "skipped": int, "notes_added": int, "history_added": int}.
    """
    from storage.database import get_db, init_db
    from storage.models import CRMNote, Lead, PriceHistory

    init_db()

    created = skipped = notes_added = history_added = 0

    with get_db() as db:
        if clear_existing:
            # Only remove demo leads — real scraped data is never touched
            demo_ids = [row[0] for row in db.query(Lead.id).filter(Lead.is_demo == True).all()]
            if demo_ids:
                db.query(CRMNote    ).filter(CRMNote.lead_id    .in_(demo_ids)).delete(synchronize_session=False)
                db.query(PriceHistory).filter(PriceHistory.lead_id.in_(demo_ids)).delete(synchronize_session=False)
                db.query(Lead).filter(Lead.is_demo == True).delete(synchronize_session=False)
            db.flush()

        for i, data in enumerate(LEADS):
            fp = _fp(data["typology"], data["zone"], str(int(data["price"] / 1000)),
                     str(int((data.get("area_m2") or 0) / 5)), f"demo-{i+1:03d}")

            existing = db.query(Lead).filter(Lead.fingerprint == fp).first()
            if existing:
                skipped += 1
                continue

            lead = Lead(
                fingerprint=fp,
                is_demo=True,           # ← all seeded leads are demo data
                title=data["title"],
                typology=data["typology"],
                zone=data["zone"],
                area_m2=data.get("area_m2"),
                price=data["price"],
                price_per_m2=data.get("price_per_m2"),
                price_benchmark=data.get("price_benchmark"),
                price_delta_pct=data.get("price_delta_pct"),
                condition=data.get("condition"),
                description=data.get("description"),
                address=data.get("address"),
                contact_name=data.get("contact_name"),
                contact_phone=data.get("contact_phone"),
                is_owner=data.get("is_owner", False),
                agency_name=data.get("agency_name"),
                days_on_market=data.get("days_on_market", 0),
                price_changes=data.get("price_changes", 0),
                score=data["score"],
                score_label=data["score_label"],
                score_breakdown=json.dumps(data.get("score_breakdown", {})),
                scored_at=datetime.utcnow(),
                crm_stage=data.get("crm_stage", "novo"),
                priority_flag=data.get("priority_flag", False),
                first_seen_at=data.get("first_seen_at", datetime.utcnow()),
                last_seen_at=data.get("first_seen_at", datetime.utcnow()),
                created_at=data.get("first_seen_at", datetime.utcnow()),
            )
            lead.sources = data.get("sources", [])
            db.add(lead)
            db.flush()

            # Price history
            for old_price in data.get("price_history", []):
                ph = PriceHistory(lead_id=lead.id, price=old_price, source="demo",
                                  recorded_at=_ago(data.get("days_on_market", 0) - 5))
                db.add(ph)
                history_added += 1

            # CRM Notes
            for note_data in data.get("notes", []):
                note = CRMNote(
                    lead_id=lead.id,
                    note=note_data["note"],
                    note_type=note_data.get("note_type", "internal"),
                    created_by="Nuno Reis",
                    created_at=_ago(max(1, data.get("days_on_market", 1) - 3)),
                )
                db.add(note)
                notes_added += 1

            created += 1

    return {"created": created, "skipped": skipped, "notes_added": notes_added, "history_added": history_added}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Seed demo data")
    parser.add_argument("--clear", action="store_true", help="Clear existing leads before seeding")
    args = parser.parse_args()

    result = run_seed(clear_existing=args.clear)
    print(f"✓ Demo data loaded:")
    print(f"  Leads created:   {result['created']}")
    print(f"  Leads skipped:   {result['skipped']} (already exist)")
    print(f"  CRM notes:       {result['notes_added']}")
    print(f"  Price history:   {result['history_added']}")
