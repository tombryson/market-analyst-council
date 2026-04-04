from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import httpx

from .contracts import DocumentRef, ParsedDocument


class DocumentParser(ABC):
    @property
    @abstractmethod
    def parser_id(self) -> str:
        raise NotImplementedError

    @abstractmethod
    async def parse_document(
        self,
        doc_ref: DocumentRef,
        *,
        client: Optional[httpx.AsyncClient] = None,
    ) -> ParsedDocument:
        raise NotImplementedError
