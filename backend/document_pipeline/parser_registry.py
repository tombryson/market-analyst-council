from __future__ import annotations

from .parsers.liteparse_parser import LiteParseDocumentParser
from .parser_base import DocumentParser
from .parsers.llamaparse_parser import LlamaParseDocumentParser
from .parsers.local_parser import LocalDocumentParser


def get_document_parser(parser_id: str) -> DocumentParser:
    key = str(parser_id or "local_default").strip().lower()
    if key in {"local_default", "local", "pymupdf"}:
        return LocalDocumentParser()
    if key in {"liteparse", "lite"}:
        return LiteParseDocumentParser()
    if key in {"llamaparse", "llama_parse", "llama"}:
        return LlamaParseDocumentParser()
    raise ValueError(f"Unknown document parser: {parser_id}")
