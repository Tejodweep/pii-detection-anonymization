"""
pii_processor.py
----------------
Core public API class.  This is what other developers import and use.

Example
-------
    from pii_detection_anonymization import PIIProcessor

    processor = PIIProcessor(strategy="regex")
    result_df = processor.process(df, column="notes")
"""

from pyspark.sql import DataFrame

from .detectors import RegexDetector, NLPDetector
from .masking import PIIMasker


# ---------------------------------------------------------------------------
# Strategy registry — add new strategies here as the library grows
# ---------------------------------------------------------------------------

_STRATEGIES = {
    "regex": RegexDetector,
    "nlp":   NLPDetector,
}


class PIIProcessor:
    """
    High-level API that wires together detection and masking.

    Parameters
    ----------
    strategy   : "regex"  — fast, rule-based detection (default)
                 "nlp"    — spaCy NER-based detection (requires spaCy install)
    pii_types  : List of PII labels to mask, e.g. ["EMAIL", "PHONE"].
                 None means mask all detected types.

    Usage
    -----
        processor = PIIProcessor(strategy="regex")
        result_df = processor.process(df, column="message")

        # result_df has three new columns:
        #   message_pii_types  — array of detected PII labels
        #   message_has_pii    — boolean flag
        #   message_masked     — text with PII replaced by placeholders
    """

    def __init__(self, strategy: str = "regex", pii_types: list = None):
        if strategy not in _STRATEGIES:
            raise ValueError(
                f"Unknown strategy '{strategy}'. "
                f"Choose from: {list(_STRATEGIES.keys())}"
            )

        self.strategy_name = strategy
        self.detector = _STRATEGIES[strategy]()
        self.masker = PIIMasker(pii_types=pii_types)

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def detect(self, df: DataFrame, column: str) -> DataFrame:
        """
        Run only the detection step.
        Adds `<column>_pii_types` and `<column>_has_pii` columns.
        """
        return self.detector.detect(df, column)

    def mask(self, df: DataFrame, column: str, output_column: str = None) -> DataFrame:
        """
        Run only the masking step (no detection columns added).
        Adds `<column>_masked` (or `output_column`) to the DataFrame.
        """
        return self.masker.mask(df, column, output_column)

    def process(self, df: DataFrame, column: str) -> DataFrame:
        """
        Run detection **and** masking in one call.
        Adds three new columns to the DataFrame.
        """
        df = self.detect(df, column)
        df = self.mask(df, column)
        return df

    def __repr__(self):
        return f"PIIProcessor(strategy='{self.strategy_name}')"