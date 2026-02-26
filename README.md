# PII Detection and Anonymization 
A **PySpark library** for detecting and masking PII (Personally Identifiable Information) in existing Spark DataFrame columns.

---

## Features

- **Two detection strategies** — switch between fast regex-based detection or smarter spaCy NLP-based detection with a single parameter change
- **Three PII types supported** — `EMAIL`, `PHONE`, and `NAME`
- **Non-destructive output** — original columns are never modified; new columns are appended to your DataFrame
- **Selective masking** — choose which PII types to redact and which to leave visible
- **Modular design** — detectors, masking logic, and the public API are fully separated for easy extension
- **Minimal dependencies** — regex strategy requires only PySpark; NLP strategy adds spaCy as an optional extra

---
## Library Usage

### 1. Import `PIIProcessor`

```python
from pii_detection_anonymization import PIIProcessor
```

That's the only import you need. `PIIProcessor` is the single public entry point for the entire library.

---

### 2. Initialize with a strategy

Choose your detection strategy when creating a `PIIProcessor` instance.

```python
# Option A — Regex strategy (recommended starting point, no extra dependencies)
processor = PIIProcessor(strategy="regex")

# Option B — NLP strategy (uses spaCy NER for smarter name detection)
processor = PIIProcessor(strategy="nlp")
```

You can change the strategy at any time simply by creating a new instance. No other code in your pipeline needs to change.

---

### 3. Process an existing Spark DataFrame column

Pass your DataFrame and the name of the text column you want to scan. The library appends new columns and returns a new DataFrame — your original is untouched.

```python
# Assume `df` is a PySpark DataFrame you already have in your pipeline
result_df = processor.process(df, column="notes")
```

**That's it.** `result_df` now contains your original data plus three new columns (see the Output Columns section below).

---

### 4. Example — Input and Output

#### Input DataFrame (`df`)

| id | notes                                                                      |
|----|----------------------------------------------------------------------------|
| 1  | Hi, I'm Ravi Kumar. Email me at Ravi.kumar@email.com or call 815-555-0192. |
| 2  | No personal info in this row at all.                                       |
| 3  | Contact Sara Jain at (800) 867-5309 for support.                           |

```python
processor = PIIProcessor(strategy="regex")
result_df = processor.process(df, column="notes")
result_df.show(truncate=False)
```

#### Output DataFrame (`result_df`)

| id | notes\_has\_pii | notes\_pii\_types | notes\_masked |
|----|----------------|------------------|--------------|
| 1  | true | ["NAME", "EMAIL", "PHONE"] | Hi, I'm [NAME]. Email me at [EMAIL] or call [PHONE]. |
| 2  | false | [] | No personal info in this row at all. |
| 3  | true | ["NAME", "PHONE"] | Contact [NAME] at [PHONE] for support. |

---

### 5. Retrieve only the masked data

If you only need the masked text and don't need the detection metadata columns, use `mask()` directly:

```python
processor = PIIProcessor(strategy="regex")

# Returns the original DataFrame plus only the masked text column
masked_df = processor.mask(df, column="notes")
masked_df.select("id", "notes_masked").show(truncate=False)
```

| id | notes\_masked |
|----|--------------|
| 1  | Hi, I'm [NAME]. Email me at [EMAIL] or call [PHONE]. |
| 2  | No personal info in this row at all. |
| 3  | Contact [NAME] at [PHONE] for support. |

---

### 6. Selective masking — redact only specific PII types

By default, all detected PII types are masked. You can restrict masking to specific types using the `pii_types` parameter:

```python
# Mask emails and phone numbers only — leave names untouched
processor = PIIProcessor(strategy="regex", pii_types=["EMAIL", "PHONE"])
result_df = processor.process(df, column="notes")
```

| id | notes\_masked |
|----|--------------|
| 1  | Hi, I'm John Smith. Email me at [EMAIL] or call [PHONE]. |
| 2  | No personal info in this row at all. |
| 3  | Contact Sarah Johnson at [PHONE] for support. |

---

### 7. Run detection without masking

If you want to flag and classify PII without modifying the text, use `detect()` on its own:

```python
processor = PIIProcessor(strategy="regex")

detected_df = processor.detect(df, column="notes")
detected_df.select("id", "notes_has_pii", "notes_pii_types").show(truncate=False)
```

---

## Output Columns Explained

Every call to `process()` or `detect()` appends two detection columns. Every call to `process()` or `mask()` appends one masking column. Column names are always prefixed with the name of the input column you passed in.

| Column | Type | Description |
|--------|------|-------------|
| `<column>_has_pii` | `boolean` | `true` if at least one PII pattern was detected in that row |
| `<column>_pii_types` | `array<string>` | List of PII labels found — e.g. `["EMAIL", "PHONE"]`. Empty array if nothing was found. |
| `<column>_masked` | `string` | A copy of the original text with detected PII replaced by placeholder tokens: `[EMAIL]`, `[PHONE]`, `[NAME]` |

**Example:** If your column is named `"customer_message"`, the new columns will be named `customer_message_has_pii`, `customer_message_pii_types`, and `customer_message_masked`.

---

## How Strategy Switching Works

This library uses the **Strategy design pattern** to make detection approaches interchangeable.

The core idea: `PIIProcessor` doesn't care *how* PII is detected — it just calls `detect()` on whatever detector object it's holding. You control which detector it holds by passing `strategy=` at initialization.

```
PIIProcessor(strategy="regex")  →  self.detector = RegexDetector()
PIIProcessor(strategy="nlp")    →  self.detector = NLPDetector()
```

Both `RegexDetector` and `NLPDetector` inherit from the same abstract `BaseDetector` class and implement an identical `detect(df, column)` method signature. This means `PIIProcessor` can call `self.detector.detect(...)` without knowing or caring which concrete class is behind it.

**The practical benefit:** switching from regex to NLP (or to any future strategy) requires changing exactly one word in your code — the `strategy=` argument. Nothing else in your pipeline needs to change.

```python
# Before
processor = PIIProcessor(strategy="regex")

# After — full NLP detection, zero other changes needed elsewhere
processor = PIIProcessor(strategy="nlp")
```

**Adding a new strategy** is equally straightforward:

1. Create a new class (e.g. `CloudDetector`) that extends `BaseDetector`
2. Implement the `detect(df, column)` method
3. Register it in the `_STRATEGIES` dictionary in `pii_processor.py`

```python
# pii_processor.py — one-line addition, nothing else changes
_STRATEGIES = {
    "regex": RegexDetector,
    "nlp":   NLPDetector,
    "cloud": CloudDetector,   # ← new strategy registered here
}
```

---

## Method Reference

| Method | Parameters | Returns | Description |
|--------|-----------|---------|-------------|
| `PIIProcessor(strategy, pii_types)` | `strategy`: `"regex"` or `"nlp"` · `pii_types`: optional list | `PIIProcessor` | Initialize with a chosen strategy |
| `.process(df, column)` | `df`: DataFrame · `column`: str | DataFrame | Detect **and** mask in one call — adds all three output columns |
| `.detect(df, column)` | `df`: DataFrame · `column`: str | DataFrame | Detection only — adds `_has_pii` and `_pii_types` columns |
| `.mask(df, column)` | `df`: DataFrame · `column`: str | DataFrame | Masking only — adds `_masked` column, no detection columns |

---

## Requirements

| Dependency | Version | Required for |
|-----------|---------|-------------|
| Python | ≥ 3.8 | Core |
| PySpark | ≥ 3.3.0 | Core |
| spaCy | ≥ 3.5.0 | `strategy="nlp"` only |
| en_core_web_sm | any | `strategy="nlp"` only |

---

## About This Project

This library was built to demonstrate:

- **Modular Python package design** with clean separation of concerns
- **The Strategy design pattern** applied to a data engineering problem
- **PySpark UDF integration**, including a lazy-loading pattern for expensive objects like spaCy models
- **Installable Python packaging** using `pyproject.toml`