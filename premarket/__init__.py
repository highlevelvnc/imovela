"""
Pre-market signal detection module.

Identifies property owners likely to sell in the near term, before any
listing appears on OLX, Imovirtual or Idealista.

Signal sources:
  - renovation_ads    : OLX / CustoJusto ads where homeowners seek contractors
  - building_permits  : CM Lisboa open data — recent permits for works on residential units
  - linkedin_search   : DuckDuckGo-mediated search for LinkedIn career/relocation changes

Entry point:  premarket.enricher.PremktEnricher
CLI:          python main.py premarket
Scheduler:    weekly Sunday 07:00 (Europe/Lisbon)
"""
