"""Document pipeline contracts and parsing services."""

from .contracts import DocumentRef, ParsedDocument
from .parse_service import parse_documents

__all__ = ["DocumentRef", "ParsedDocument", "parse_documents"]
