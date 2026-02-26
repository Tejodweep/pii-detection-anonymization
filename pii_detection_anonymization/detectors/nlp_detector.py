"""
nlp_detector.py
---------------
Detects PII using spaCy's named-entity recognition (NER).

spaCy entity labels mapped to our PII types:
  PERSON  → NAME
  GPE     → LOCATION  (geo-political entity: cities, countries, states)
  LOC     → LOCATION  (non-GPE locations, mountain ranges, etc.)
  ORG     → skipped   (organisations are not PII in this library's scope)

Emails and phones are not recognised by spaCy NER, so we fall back to
regex helpers for those two types.

Requirements:
    pip install spacy
    python -m spacy download en_core_web_sm
"""

import re
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

from .base_detector import BaseDetector

# Lazy-load spaCy so the library can be imported without spaCy installed
# (as long as the user never instantiates NLPDetector).
try:
    import spacy
    _SPACY_AVAILABLE = True
except ImportError:
    _SPACY_AVAILABLE = False


# ---------------------------------------------------------------------------
# Regex fallbacks for email / phone (spaCy NER won't catch these)
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
_PHONE_RE = re.compile(r"(\+?\d{1,3}[\s\-.])?(\(?\d{3}\)?[\s\-.])\d{3}[\s\-.]\d{4}")

# Map spaCy labels → our PII type labels
_LABEL_MAP = {
    "PERSON": "NAME",
    "GPE":    "LOCATION",
    "LOC":    "LOCATION",
}


# ---------------------------------------------------------------------------
# UDF helper — module-level cache so spaCy loads once per executor
# ---------------------------------------------------------------------------

_nlp_model = None  # loaded lazily, cached per Python process


def _get_nlp():
    global _nlp_model
    if _nlp_model is None:
        _nlp_model = spacy.load("en_core_web_sm")
    return _nlp_model


def _nlp_find_pii(text: str) -> list:
    """Pure Python function wrapped as a Spark UDF below."""
    if not text:
        return []

    nlp = _get_nlp()
    doc = nlp(text)
    found = set()

    # NER pass — names and locations
    for ent in doc.ents:
        pii_type = _LABEL_MAP.get(ent.label_)
        if pii_type:
            found.add(pii_type)

    # Regex pass — emails and phones
    if _EMAIL_RE.search(text):
        found.add("EMAIL")
    if _PHONE_RE.search(text):
        found.add("PHONE")

    return list(found)


_nlp_pii_udf = F.udf(_nlp_find_pii, ArrayType(StringType()))


# ---------------------------------------------------------------------------
# Detector class
# ---------------------------------------------------------------------------

class NLPDetector(BaseDetector):
    """
    PII detector powered by spaCy NER + regex fallbacks.

    Args:
        model: spaCy model name to load (default: "en_core_web_sm").
               Make sure the model is downloaded:
                   python -m spacy download en_core_web_sm
    """

    def __init__(self, model: str = "en_core_web_sm"):
        if not _SPACY_AVAILABLE:
            raise ImportError(
                "spaCy is not installed. Run: pip install spacy && "
                "python -m spacy download en_core_web_sm"
            )
        self._model = model  # stored for reference, actual load happens inside UDF

    def detect(self, df: DataFrame, column: str) -> DataFrame:
        pii_col     = f"{column}_pii_types"
        has_pii_col = f"{column}_has_pii"

        df = df.withColumn(pii_col, _nlp_pii_udf(F.col(column)))
        df = df.withColumn(has_pii_col, F.size(F.col(pii_col)) > 0)
        return df