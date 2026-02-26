"""
regex_detector.py
-----------------
Detects PII using regular expressions.
Covers: emails, phone numbers, names, and locations.
"""

import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from .base_detector import BaseDetector


# ---------------------------------------------------------------------------
# Pattern definitions
# ---------------------------------------------------------------------------

PATTERNS = {
    "EMAIL":    r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
    "PHONE":    r"(\+?\d{1,3}[\s\-.])?(\(?\d{3}\)?[\s\-.])\d{3}[\s\-.]\d{4}",
    "NAME":     r"\b[A-Z][a-z]+\s[A-Z][a-z]+\b",
    "LOCATION": r"\b(?:in|at|from|near|city|town|village)\s[A-Z][a-zA-Z]+\b",
}


# ---------------------------------------------------------------------------
# UDF — runs on each row inside Spark executors
# ---------------------------------------------------------------------------

def _find_pii_types(text: str) -> list:
    if not text:
        return []
    found = []
    for label, pattern in PATTERNS.items():
        if re.search(pattern, text):
            found.append(label)
    return found


_find_pii_udf = F.udf(_find_pii_types, ArrayType(StringType()))


# ---------------------------------------------------------------------------
# Detector class
# ---------------------------------------------------------------------------

class RegexDetector(BaseDetector):
    """Detects PII using regular expressions."""

    def detect(self, df: DataFrame, column: str) -> DataFrame:
        pii_col     = f"{column}_pii_types"
        has_pii_col = f"{column}_has_pii"

        df = df.withColumn(pii_col, _find_pii_udf(F.col(column)))
        df = df.withColumn(has_pii_col, F.size(F.col(pii_col)) > 0)
        return df