# PII Detection and Anonymization 
A PySpark-based library for detecting and masking PII (Personally Identifiable Information) in structured tabular datasets using:

Regex-based pattern matching

NLP-based Named Entity Recognition (NER) with spaCy

## Detected PII Types

| PII Type  | Regex Detection | NLP Detection (spaCy) |
|------------|----------------|------------------------|
| Email      | ✅             | ✅                     |
| Phone      | ✅             | ✅                     |
| Aadhaar    | ✅             | ✅                     |
| PAN        | ✅             | ✅                     |
| Name       | ✅ (pattern-based) | ✅ (NER-based)     |


# Library Usage

This section provides a complete, step-by-step guide - from installation to generating  masked output.

---

## Prerequisites

Make sure the following are installed:

| Requirement | Minimum Version | Notes |
|-------------|----------------|-------|
| Python      | 3.8+           | Required to run the library |
| Java (JDK)  | 8 or 11        | Required by Apache Spark |
| pip         | Latest         | Run `pip install --upgrade pip` |


---

### Step 1 — Clone the Repository

```bash
git clone https://github.com/Tejodweep/pii-detection-anonymization.git
cd pii-detection-anonymization
```

---


### Step 2 — Install the Library

**Option A — Regex only** *(no extra dependencies)*

```bash
pip install -e .
```

**Option B — Regex + NLP (spaCy)**

```bash
pip install -e ".[nlp]"
```

> The `-e` flag installs the package in *editable mode*, meaning any changes you make to the source code are instantly reflected without reinstalling.

---

### Step 3 — Download the spaCy Language Model *(NLP strategy only)*

If you installed the `[nlp]` extra above, you must also download the English language model that spaCy uses to recognise person names:

```bash
python -m spacy download en_core_web_sm
```

> **What is this?** `en_core_web_sm` is a small pre-trained English NLP model. It powers the Named Entity Recognition (NER) that detects person names in free-form text. This step is **not needed** if you only plan to use the `regex` strategy.

---

### Step 4 — Prepare Your Input File

The library accepts **CSV**, **JSON**, and **Parquet** files. Here is a minimal CSV example to get started:

**`data/customers.csv`**
```csv
id,name,notes
1,Amit Sharma,"Reach Amit at 9876543210 or amit.sharma@example.com"
2,Priya Nair,"PAN: ABCDE1234F  Aadhaar: 1234 5678 9012"
3,Unknown User,"No personal data here — just a regular note."
```

---

### Step 5 — Run the Pipeline

#### Using the Regex Strategy *(fast, no spaCy needed)*

```python
from pii_detection_anonymization import PIIDetectionAnonymization

# Initialise the pipeline with regex-based detection
pipeline = PIIDetectionAnonymization(strategy="regex")

# Read the file, detect PII in the chosen columns, return masked DataFrame
result = pipeline.run(
    file_path="data/customers.csv",
    columns=["name", "notes"],  # which columns may contain PII
    file_format="csv",
)

result.show(truncate=False)
```

**Expected output:** 

```
+---+---------------+------------------------------+
|id |name           |notes                         |
+---+---------------+------------------------------+
|1  |[MASKED-NAME]  |[MASKED-EMAIL]                |
|2  |[MASKED-NAME]  |[MASKED-AADHAAR]              |
|3  |Unknown User   |No personal data here ...     |
+---+---------------+------------------------------+
```
> **How masking priority works:** When multiple PII types appear in the same cell, the most sensitive type wins: `Aadhaar > PAN > Email > Phone > Name`.

---

#### Using the NLP Strategy *(context-aware name detection)*

```python
from pii_detection_anonymization import PIIDetectionAnonymization

# Switch strategy with one argument — everything else stays the same
pipeline = PIIDetectionAnonymization(strategy="nlp")

result = pipeline.run(
    file_path="data/customers.csv",
    columns=["name", "notes"],
    file_format="csv",
)

result.show(truncate=False)
```

> **Regex vs NLP — which should I use?**
>
> | | `regex` | `nlp` |
> |---|---|---|
> | Speed | Fast | Slower (model load per row) |
> | Best for | Structured PII (email, phone, Aadhaar, PAN) | Free-form text with person names |
> | Extra setup | None | spaCy model required |
>
> Start with `regex`. Switch to `nlp` only if you have free-form sentences where names appear in context (e.g., *"Send the report to John Smith"*).

---

### Step 6 — Save the Masked Output

Pass `output_path` and `output_format` to write the masked result to disk:

```python
pipeline.run(
    file_path="data/customers.csv",
    columns=["name", "notes"],
    file_format="csv",
    output_path="output/masked_customers",  # Spark writes a folder of part files here
    output_format="csv",
)
```

> **Why does Spark write a folder, not a single file?** Spark processes data in parallel partitions, each writing its own output file — this is what makes it fast at scale. To get a single file locally, you can call `.coalesce(1)` before writing, but avoid this on large datasets.

---

### Step 7 — Run on an Existing DataFrame *(advanced)*

Already have a Spark DataFrame loaded from Delta Lake, a database, or another source? Skip the file reader entirely:

```python
from pii_detection_anonymization import PIIDetectionAnonymization
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").master("local[*]").getOrCreate()

df = spark.read.parquet("s3://my-bucket/raw/users/")

pipeline = PIIDetectionAnonymization(strategy="regex")
masked_df = pipeline.run_on_dataframe(df, columns=["email", "bio"])

masked_df.show()
```

---

### Supported File Formats

| Format  | `file_format` value | Notes |
|---------|---------------------|-------|
| CSV     | `"csv"`             | Header row auto-detected |
| JSON    | `"json"`            | One JSON object per line (JSON Lines) |
| Parquet | `"parquet"`         | Columnar format, best for large datasets |

---

### Complete API Reference

```python
# Initialise
PIIDetectionAnonymization(
    strategy="regex",          # "regex" or "nlp"
    spark_master="local[*]",   # Spark master URL; swap for a cluster URL in production
    app_name="PII-Pipeline",   # Name shown in the Spark UI
)

# Run from a file
pipeline.run(
    file_path="...",           # Path to input file
    columns=["col1", "col2"],  # Columns to scan for PII
    file_format="csv",         # "csv", "json", or "parquet"
    output_path=None,          # Optional: folder path to write masked output
    output_format="csv",       # Format for the output (if output_path is set)
)

# Run on an existing DataFrame
pipeline.run_on_dataframe(
    df=existing_df,
    columns=["col1"],
)
```

---

## How the Strategy Pattern Works

```
PIIDetectionAnonymization          ← Context (your single entry point)
       │
       │ delegates detection to
       ▼
   BaseDetector (ABC)              ← Strategy interface
       │
       ├── RegexDetector           ← strategy="regex"
       └── NLPDetector             ← strategy="nlp"
```

The main class depends only on the abstract `BaseDetector` interface. Switching from Regex to NLP is a **single argument change** — no other code is touched. Adding a new strategy (e.g., an ML-based detector) means creating one new subclass and registering it in `_STRATEGY_REGISTRY`.

---

## Performance Notes

- **RegexDetector** is fast and scales naturally with Spark's parallelism.
- **NLPDetector** currently loads the spaCy model inside the UDF on every row, which is slow on large datasets. A known improvement is to use `mapInPandas` (Pandas UDF) to load the model once per partition. This is tracked as a future improvement.

---

## Running Tests

```bash
pip install -e ".[dev]"
pytest tests/ -v
```

---

## Future Improvements

- [ ] `mapInPandas` optimisation for NLPDetector (load spaCy model once per partition)
- [ ] Substring-level masking (redact only the matched PII span, not the entire cell)
- [ ] Delta Lake output support
- [ ] Configurable mask labels per PII type
- [ ] CLI interface using `argparse` or `typer`
- [ ] GitHub Actions CI pipeline

---

