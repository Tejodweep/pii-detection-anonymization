"""
example_usage.py
----------------
Shows how another developer would import and use pii_detection_anonymization.

Run with:
    python example_usage.py

Requirements:
    pip install pyspark
    pip install spacy                   # only needed for strategy="nlp"
    python -m spacy download en_core_web_sm   # only needed for strategy="nlp"
"""

# ── 1. Imports ──────────────────────────────────────────────────────────────
from pii_detection_anonymization import PIIProcessor
from pii_detection_anonymization.utils import get_or_create_spark

# ── 2. Create (or reuse) a SparkSession ─────────────────────────────────────
spark = get_or_create_spark("PIIDemo")

# ── 3. Build a sample DataFrame (in practice you already have one) ──────────
sample_data = [
    (1, "Hi, I'm Ravi Kumar. Reach me at ravi.kumar@email.com or 815-555-0192."),
    (2, "No personal info in this row at all."),
    (3, "Contact Sarah Johnson at (800) 867-5309 for support."),
    (4, "Invoice sent to billing@acme.org — see attached."),
]
df = spark.createDataFrame(sample_data, schema=["id", "notes"])

print("=== Original DataFrame ===")
df.show(truncate=False)


# ── 4a. Use the REGEX strategy (no extra dependencies) ──────────────────────
processor = PIIProcessor(strategy="regex")
result_df = processor.process(df, column="notes")

print("=== After process() — Regex Strategy ===")
result_df.select("id", "notes_has_pii", "notes_pii_types", "notes_masked").show(
    truncate=False
)


# ── 4b. Use only DETECTION (no masking) ─────────────────────────────────────
detected_df = processor.detect(df, column="notes")
print("=== Detection only ===")
detected_df.select("id", "notes_has_pii", "notes_pii_types").show(truncate=False)


# ── 4c. Use only MASKING (skip detection columns) ────────────────────────────
masked_only_df = processor.mask(df, column="notes")
print("=== Masking only ===")
masked_only_df.select("id", "notes_masked").show(truncate=False)


# ── 4d. Mask SPECIFIC PII types only (e.g., keep names, hide contact info) ───
selective_processor = PIIProcessor(strategy="regex", pii_types=["EMAIL", "PHONE"])
selective_df = selective_processor.process(df, column="notes")
print("=== Selective masking — EMAIL and PHONE only ===")
selective_df.select("id", "notes_masked").show(truncate=False)


# ── 4e. Switch to NLP strategy (requires spaCy) ─────────────────────────────
# Uncomment once spaCy is installed:
#
# nlp_processor = PIIProcessor(strategy="nlp")
# nlp_result_df = nlp_processor.process(df, column="notes")
# nlp_result_df.select("id", "notes_has_pii", "notes_pii_types", "notes_masked").show(
#     truncate=False
# )

spark.stop()