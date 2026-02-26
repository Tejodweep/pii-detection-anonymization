"""
base_detector.py
Defines the abstract base class for all PII detection strategies.
"""

from abc import ABC, abstractmethod
from typing import List, Tuple


class BaseDetector(ABC):
    """
    Abstract base class that all detection strategies must implement.
    Each detector receives a text string and returns a list of
    (matched_text, pii_type) tuples.
    """

    @abstractmethod
    def detect(self, text: str) -> List[Tuple[str, str]]:
        """
        Detect PII entities in a text string.

        Args:
            text: The input string to scan.

        Returns:
            A list of tuples: [(matched_text, pii_type), ...]
            e.g. [("John Smith", "NAME"), ("john@example.com", "EMAIL")]
        """
        raise NotImplementedError
