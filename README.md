# PII Detection and Anonymization 

A  Personal Identifiable Information (PII) detection and anonymization system 
that allows users to detect sensitive data using either:

- Regex-based pattern matching
- NLP-based Named Entity Recognition (NER)

The system can identify and anonymize entities such as emails, phone numbers, names, and locations.

## Features

- Regex-based PII detection
- NLP-based PII detection (spaCy NER)
- Supports anonymization / masking of detected entities
- Modular and extensible architecture
- Clean separation between detection and redaction


## Tech Stack

- Python 3.x
- Regex (re module)
- spaCy (for NLP-based NER)

## Supported PII Entities

- Email addresses
- Phone numbers
- Names
- Locations

## Usage

Run the program:
python3 main.py




