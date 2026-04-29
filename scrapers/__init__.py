from .olx import OLXScraper
from .olx_marketplace import OLXMarketplaceScraper
from .standvirtual import StandvirtualScraper
from .linkedin import LinkedInScraper
from .imovirtual import ImovirtualScraper
from .idealista import IdealistaScraper
from .sapo import SapoScraper
from .custojusto import CustojustoScraper

# Bank-owned real-estate portals (REOs) — opt-in via sources arg
from .banks import (
    CGDImoveisScraper,
    MillenniumImoveisScraper,
    NovobancoImoveisScraper,
    SantanderImoveisScraper,
)

# Auctions and Facebook Marketplace
from .leiloes import LeiloesScraper
from .facebook_marketplace import FacebookMarketplaceScraper

__all__ = [
    "OLXScraper",
    "OLXMarketplaceScraper",
    "StandvirtualScraper",
    "LinkedInScraper",
    "ImovirtualScraper",
    "IdealistaScraper",
    "SapoScraper",
    "CustojustoScraper",
    "CGDImoveisScraper",
    "MillenniumImoveisScraper",
    "NovobancoImoveisScraper",
    "SantanderImoveisScraper",
    "LeiloesScraper",
    "FacebookMarketplaceScraper",
]
