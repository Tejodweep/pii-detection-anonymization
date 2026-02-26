"""
masker.py
---------
Replaces detected PII in a text column with placeholder tokens.

Default placeholders:
  EMAIL  → [EMAIL]
  PHONE  → [PHONE]
  NAME   → [NAME]

Usage:
  masker = PIIMasker()                                      # mask all types
  masker = PIIMasker(pii_types=["EMAIL", "PHONE"])          # selective masking
  masker = PIIMasker(mask_map={"EMAIL": (pattern, "***")})  # custom placeholder
"""

import re
from typing import Optional, Dict, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


# ---------------------------------------------------------------------------
# Default mask map
# ---------------------------------------------------------------------------

DEFAULT_MASK_MAP: Dict[str, Tuple[str, str]] = {
    "EMAIL": (r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", "[EMAIL]"),
    "PHONE": (r"(\+?1[\s\-.]?)?\(?\d{3}\)?[\s\-.]?\d{3}[\s\-.]?\d{4}", "[PHONE]"),
    "NAME":  (r"\b[A-Z][a-z]+\s[A-Z][a-z]+\b", "[NAME]"),
}


# ---------------------------------------------------------------------------
# Masker class
# ---------------------------------------------------------------------------

class PIIMasker:
    """
    Masks PII found in a DataFrame column by replacing it with placeholder tokens.

    Parameters
    ----------
    mask_map   : Optional dict to override or extend default patterns/placeholders.
                 Format: {"PII_TYPE": (regex_pattern, replacement_string)}
    pii_types  : Optional list of PII types to mask, e.g. ["EMAIL", "PHONE"].
                 If provided, only these types will be masked. Others are ignored.
                 If None, all types in mask_map are masked.
    """

    def __init__(
        self,
        mask_map: Optional[Dict[str, Tuple[str, str]]] = None,
        pii_types: Optional[list] = None
    ):
        # Merge user overrides on top of defaults
        self._mask_map = {**DEFAULT_MASK_MAP, **(mask_map or {})}

        # Filter to only requested PII types (computed once at init, not per row)
        if pii_types:
            self._mask_map = {
                k: v for k, v in self._mask_map.items() if k in pii_types
            }

    def mask(
        self,
        df: DataFrame,
        column: str,
        output_column: str = None
    ) -> DataFrame:
        """
        Apply masking to a DataFrame column.

        Parameters
        ----------
        df            : Input PySpark DataFrame
        column        : Name of the column containing raw text
        output_column : Name of the output column (default: <column>_masked)

        Returns
        -------
        DataFrame with a new masked text column appended.
        """
        out_col = output_column or f"{column}_masked"
        mask_map = self._mask_map  # capture for UDF closure

        def _mask_text(text: str) -> str:
            if not text:
                return text
            for pattern, replacement in mask_map.values():
                text = re.sub(pattern, replacement, text)
            return text

        mask_udf = F.udf(_mask_text, StringType())
        return df.withColumn(out_col, mask_udf(F.col(column)))