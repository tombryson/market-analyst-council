"""PDF processing for text extraction and intelligent chunking."""

import os
import re
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

try:
    import pymupdf  # PyMuPDF
    PYMUPDF_AVAILABLE = True
    PYMUPDF_IMPORT_ERROR = None
except ImportError as e:
    PYMUPDF_AVAILABLE = False
    PYMUPDF_IMPORT_ERROR = f"ImportError: {e}"
    print(f"Warning: PyMuPDF unavailable ({PYMUPDF_IMPORT_ERROR}). PDF processing will fail.")
except Exception as e:
    PYMUPDF_AVAILABLE = False
    PYMUPDF_IMPORT_ERROR = f"{type(e).__name__}: {e}"
    print(f"Warning: PyMuPDF unavailable ({PYMUPDF_IMPORT_ERROR}). PDF processing will fail.")

from .config import MAX_PDF_SIZE_MB, MAX_PDF_PAGES_FULL_TEXT, ATTACHMENTS_DIR
from .openrouter import query_model


# Configuration constants
CHUNK_SIZE_CHARS = 4000  # Characters per chunk for large documents
MIN_CHUNK_OVERLAP = 200  # Character overlap between chunks


async def extract_text_from_pdf(file_path: str) -> Dict[str, Any]:
    """
    Extract text from PDF file using PyMuPDF.

    Args:
        file_path: Path to PDF file

    Returns:
        Dict with:
        - 'text': Full extracted text
        - 'page_count': Number of pages
        - 'metadata': PDF metadata (title, author, etc.)
        - 'extraction_method': 'pymupdf'
        - 'error': Error message if extraction failed
    """
    if not PYMUPDF_AVAILABLE:
        return {
            "text": "",
            "page_count": 0,
            "metadata": {},
            "extraction_method": "none",
            "error": f"PyMuPDF unavailable: {PYMUPDF_IMPORT_ERROR or 'unknown import error'}"
        }

    try:
        doc = pymupdf.open(file_path)

        # Extract metadata
        metadata = {
            "title": doc.metadata.get("title", ""),
            "author": doc.metadata.get("author", ""),
            "subject": doc.metadata.get("subject", ""),
        }

        # Extract text from all pages
        text_parts = []
        for page_num in range(len(doc)):
            page = doc[page_num]
            text_parts.append(page.get_text())

        full_text = "\n\n".join(text_parts)
        page_count = len(doc)

        doc.close()

        return {
            "text": full_text,
            "page_count": page_count,
            "metadata": metadata,
            "extraction_method": "pymupdf"
        }

    except Exception as e:
        return {
            "text": "",
            "page_count": 0,
            "metadata": {},
            "extraction_method": "none",
            "error": f"PDF extraction failed: {str(e)}"
        }


def chunk_text_intelligently(
    text: str,
    chunk_size: int = CHUNK_SIZE_CHARS
) -> List[str]:
    """
    Split text into overlapping chunks with semantic boundaries.

    Args:
        text: Full document text
        chunk_size: Target characters per chunk

    Returns:
        List of text chunks with overlap
    """
    if len(text) <= chunk_size:
        return [text]

    chunks = []

    # Split on double newlines (paragraphs) first
    paragraphs = text.split('\n\n')

    current_chunk = []
    current_size = 0

    for para in paragraphs:
        para_size = len(para)

        if current_size + para_size > chunk_size and current_chunk:
            # Save current chunk
            chunks.append('\n\n'.join(current_chunk))

            # Start new chunk with overlap (keep last paragraph)
            if current_chunk:
                current_chunk = [current_chunk[-1], para]
                current_size = len(current_chunk[-1]) + para_size
            else:
                current_chunk = [para]
                current_size = para_size
        else:
            current_chunk.append(para)
            current_size += para_size

    # Add final chunk
    if current_chunk:
        chunks.append('\n\n'.join(current_chunk))

    return chunks


async def summarize_large_pdf(
    text: str,
    filename: str
) -> str:
    """
    Generate concise summary of large PDF using fast LLM.

    Args:
        text: Full PDF text
        filename: Original filename for context

    Returns:
        Concise summary (target: 500-1000 words)
    """
    # If text is very long, chunk it first and summarize chunks
    if len(text) > 30000:  # ~30k chars is too much for single summary
        chunks = chunk_text_intelligently(text, chunk_size=15000)

        # Summarize first few chunks (don't process entire huge document)
        chunk_summaries = []
        for i, chunk in enumerate(chunks[:5]):  # Max 5 chunks
            summary_prompt = f"""Summarize the following section from a document titled "{filename}". Focus on key points, main arguments, and important facts:

{chunk}

Concise summary:"""

            messages = [{"role": "user", "content": summary_prompt}]

            try:
                response = await query_model("google/gemini-2.5-flash", messages, timeout=30.0)
                if response and response.get('content'):
                    chunk_summaries.append(response['content'])
            except Exception as e:
                print(f"Chunk summary error: {str(e)}")
                continue

        # Combine chunk summaries
        combined = "\n\n".join(chunk_summaries)
        text_to_summarize = combined if combined else text[:20000]
    else:
        text_to_summarize = text

    # Final summary
    final_prompt = f"""Provide a comprehensive summary of this document titled "{filename}". Include:
- The main topic and purpose
- Key points and arguments
- Important facts and conclusions
- Any relevant data or findings

Keep the summary clear and well-organized. Target length: 500-1000 words.

Document content:
{text_to_summarize}

Summary:"""

    messages = [{"role": "user", "content": final_prompt}]

    try:
        response = await query_model("google/gemini-2.5-flash", messages, timeout=45.0)
        if response and response.get('content'):
            return response['content']
    except Exception as e:
        print(f"Final summary error: {str(e)}")

    # Fallback: return truncated text
    return f"[Summary generation failed. First 1000 characters of document]\n\n{text[:1000]}..."


async def process_pdf_attachment(
    file_path: str,
    filename: str
) -> Dict[str, Any]:
    """
    Main entry point for PDF processing.

    Args:
        file_path: Path to uploaded PDF
        filename: Original filename

    Returns:
        Dict with processing results:
        - 'filename': Original name
        - 'status': 'success' | 'failed'
        - 'text_length': Character count
        - 'page_count': Number of pages
        - 'full_text': Complete text (if pages <= MAX_PDF_PAGES_FULL_TEXT)
        - 'summary': AI summary (if pages > MAX_PDF_PAGES_FULL_TEXT)
        - 'chunks': Number of chunks created (if applicable)
        - 'error': Error message if failed
    """
    # Extract text
    extraction_result = await extract_text_from_pdf(file_path)

    if extraction_result.get('error'):
        return {
            "filename": filename,
            "status": "failed",
            "text_length": 0,
            "page_count": 0,
            "error": extraction_result['error']
        }

    text = extraction_result['text']
    page_count = extraction_result['page_count']
    text_length = len(text)

    if text_length == 0:
        return {
            "filename": filename,
            "status": "failed",
            "text_length": 0,
            "page_count": page_count,
            "error": "No text could be extracted from PDF"
        }

    # Decide processing strategy based on page count
    if page_count <= MAX_PDF_PAGES_FULL_TEXT:
        # Small PDF: use full text
        return {
            "filename": filename,
            "status": "success",
            "text_length": text_length,
            "page_count": page_count,
            "full_text": text,
            "processing_method": "full_text"
        }
    else:
        # Large PDF: generate summary
        print(f"PDF {filename} has {page_count} pages, generating summary...")
        summary = await summarize_large_pdf(text, filename)

        chunks = chunk_text_intelligently(text)

        return {
            "filename": filename,
            "status": "success",
            "text_length": text_length,
            "page_count": page_count,
            "summary": summary,
            "chunks": len(chunks),
            "processing_method": "summarized"
        }


def format_pdf_context_for_prompt(
    processed_pdfs: List[Dict[str, Any]]
) -> str:
    """
    Format processed PDF data for inclusion in LLM prompts.

    Args:
        processed_pdfs: List of results from process_pdf_attachment()

    Returns:
        Formatted markdown string with PDF content
    """
    if not processed_pdfs:
        return ""

    lines = []

    for pdf in processed_pdfs:
        filename = pdf.get('filename', 'Unknown')
        status = pdf.get('status', 'unknown')

        if status == 'failed':
            error = pdf.get('error', 'Unknown error')
            lines.append(f"**Attachment: {filename}**")
            lines.append(f"Status: Failed - {error}\n")
            continue

        page_count = pdf.get('page_count', 0)
        processing_method = pdf.get('processing_method', 'unknown')

        lines.append(f"**Attachment: {filename}** ({page_count} pages)")

        if processing_method == 'full_text' and pdf.get('full_text'):
            lines.append("Processing: Full text extracted\n")
            full_text = pdf['full_text']
            # Truncate if extremely long
            if len(full_text) > 20000:
                lines.append(full_text[:20000] + "\n\n[Document continues...]")
            else:
                lines.append(full_text)
        elif processing_method == 'summarized' and pdf.get('summary'):
            lines.append(f"Processing: AI-generated summary (document was {page_count} pages)\n")
            lines.append(pdf['summary'])

        lines.append("")  # Blank line between documents

    return "\n".join(lines)


async def save_attachment(
    file_content: bytes,
    conversation_id: str,
    message_id: str,
    filename: str
) -> str:
    """
    Save uploaded file to disk.

    Args:
        file_content: Raw file bytes
        conversation_id: Conversation ID
        message_id: Message ID (generated as UUID)
        filename: Original filename

    Returns:
        Storage path string
    """
    # Create directory structure
    attach_dir = Path(ATTACHMENTS_DIR) / conversation_id / message_id
    attach_dir.mkdir(parents=True, exist_ok=True)

    # Sanitize filename
    safe_filename = re.sub(r'[^\w\s\-\.]', '_', filename)

    file_path = attach_dir / safe_filename

    # Save file
    with open(file_path, 'wb') as f:
        f.write(file_content)

    return str(file_path)
