"""Abstract base class for SERP adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from serp_adapter.models import NormalizedSerpResult


class BaseSerpAdapter(ABC):
    """Contract that every SERP provider adapter must satisfy.

    Sub-classes implement :meth:`normalize` to translate the provider-specific
    raw response into a :class:`~serp_adapter.models.NormalizedSerpResult`.
    """

    @abstractmethod
    def normalize(self, raw: Any) -> NormalizedSerpResult:
        """Convert *raw* provider output to a :class:`NormalizedSerpResult`.

        Parameters
        ----------
        raw:
            The raw data returned by the provider.  The expected type is
            specific to each concrete adapter.

        Returns
        -------
        NormalizedSerpResult
            A fully populated normalized result.
        """
