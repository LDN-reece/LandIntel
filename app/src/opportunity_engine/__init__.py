"""Phase One opportunity engine package.

The package intentionally avoids eager runtime imports so the ranking and
briefing modules can be validated in isolation without pulling in the full
application stack.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - import for editor support only
    from src.opportunity_engine.service import OpportunityService
    from src.opportunity_engine.types import OpportunitySearchFilters

__all__ = ["OpportunitySearchFilters", "OpportunityService"]
