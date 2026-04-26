"""
CLEARLINE — BNF Chroma DB Client
==================================
Queries the locally-stored BNF 80 (Sept 2020–March 2021) vector database
for clinical guidance on drug + diagnosis combinations.

Used as supplementary evidence in ClinicalNecessityEngine — if BNF returns
useful text it is injected into the AI prompt; if it returns nothing the
engine continues without it (graceful degradation).

Quality gate:
  - Semantic distance must be < 0.55 (empirically tuned on BNF 80 corpus)
  - Cleaned chunk must contain at least one dose instruction
    (Adult: + a numeric dose, or a route marker like BY MOUTH / BY INTRAVENOUS)
  - Chunks that are predominantly price-table noise are silently dropped

The BNF is a UK reference; for Nigeria-specific first-line drugs (e.g. ACT
for malaria) it may return nothing — the engine then falls back to
RxNorm + WHO EML guidance.
"""

import re
import logging
from functools import lru_cache
from typing import Optional

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

BNF_DB_PATH  = "bnf_chroma_db"
COLLECTION   = "bnf_80"
MAX_DISTANCE = 0.55   # chunks above this are semantically too far
MAX_CHARS    = 1800   # cap on total text sent to AI

# A chunk must match at least one of these to pass the quality gate
_DOSE_PATTERN = re.compile(
    r'(?i)(adult\s*:\s*\d|child\s+\d|by\s*mouth|bymouth|'
    r'by\s*intravenous|byintravenous|by\s*intramuscular|byintramuscular|'
    r'▶\s*(by|adult)|once\s+daily|twice\s+daily|every\s+\d+\s+hours?|'
    r'\d+\s*mg\s*(once|twice|daily|every)|for\s+\d+\s+days?)'
)

# Patterns that indicate price-table contamination
_NOISE_PATTERN = re.compile(
    r'(?i)(£\d|\bDT=\b|Pößneck|DataStandards|GGPMedia|'
    r'\btypesetby\b|\bprintedby\b)'
)

# ── Lazy chromadb client (initialised once) ───────────────────────────────────

_collection = None


def _get_collection():
    global _collection
    if _collection is not None:
        return _collection
    try:
        import chromadb
        client      = chromadb.PersistentClient(path=BNF_DB_PATH)
        _collection = client.get_collection(COLLECTION)
        logger.info(f"BNF DB loaded: {_collection.count()} chunks")
        return _collection
    except Exception as e:
        logger.warning(f"BNF DB unavailable: {e}")
        return None


# ── Text cleaning (same logic as test_bnf.py) ─────────────────────────────────

def _add_spaces(text: str) -> str:
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    text = re.sub(r'([a-zA-Z])(\d)', r'\1 \2', text)
    text = re.sub(r'(\d)([a-zA-Z])(?!g\b|ml\b|mcg\b|iu\b|mg\b)', r'\1 \2', text)
    return text


def _clean_chunk(raw: str) -> str:
    """
    Extract clinical lines from a raw BNF chunk and discard price-table noise.
    Returns empty string if nothing useful survives.
    """
    segments = re.split(r'(?=▶)|(?<=\n)', raw)
    kept = []

    for seg in segments:
        seg = seg.strip()
        if len(seg) < 12:
            continue
        if _NOISE_PATTERN.search(seg):
            continue
        if _DOSE_PATTERN.search(seg):
            cleaned = _add_spaces(seg)
            cleaned = re.sub(r'\s+', ' ', cleaned).strip()
            if len(cleaned) > 15:
                kept.append(cleaned)

    return "\n".join(kept)


def _passes_quality_gate(cleaned: str, drug: str) -> bool:
    """
    Return True only if:
    1. Cleaned chunk contains actionable dosing info (Adult: dose, BY MOUTH, etc.)
    2. The drug name (or its first word, for compound names) appears in the chunk.
       This prevents chunks from adjacent BNF chapters polluting the result.
    """
    if not cleaned or not _DOSE_PATTERN.search(cleaned):
        return False

    # Accept if the drug name (or first word of a compound name) is in the text
    drug_lower   = drug.lower()
    first_word   = drug_lower.split()[0]   # "artemether" from "artemether lumefantrine"
    cleaned_lower = cleaned.lower()

    return first_word in cleaned_lower or drug_lower in cleaned_lower


# ── Public API ────────────────────────────────────────────────────────────────

def bnf_get_guidance(drug: str, diagnosis: str, n_results: int = 4) -> str:
    """
    Query BNF for clinical guidance on a drug + diagnosis combination.

    Returns a cleaned text string (≤ MAX_CHARS) suitable for injection into
    an AI prompt, or empty string if no useful guidance found.

    Never raises — all errors are logged and empty string is returned.
    """
    col = _get_collection()
    if col is None:
        return ""

    try:
        query   = f"{drug} {diagnosis} dose adult treatment"
        results = col.query(query_texts=[query], n_results=n_results)

        parts = []
        for doc, meta, dist in zip(
            results["documents"][0],
            results["metadatas"][0],
            results["distances"][0],
        ):
            if dist > MAX_DISTANCE:
                continue

            cleaned = _clean_chunk(doc)
            if not _passes_quality_gate(cleaned, drug):
                logger.debug(
                    f"BNF chunk p.{meta.get('page','?')} dist={dist:.3f} "
                    f"failed quality gate — dropped"
                )
                continue

            page = meta.get("page", "?")
            parts.append(f"[BNF 80 p.{page}]\n{cleaned}")

        if not parts:
            logger.debug(f"BNF: no quality-passing chunks for '{drug} + {diagnosis}'")
            return ""

        combined = "\n\n".join(parts)[:MAX_CHARS]
        logger.info(
            f"BNF: {len(parts)} chunk(s) returned for '{drug} + {diagnosis}'"
        )
        return combined

    except Exception as e:
        logger.warning(f"BNF query failed for '{drug} + {diagnosis}': {e}")
        return ""
