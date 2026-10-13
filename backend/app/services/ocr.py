from __future__ import annotations

import hashlib
import subprocess
import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pypdf import PdfReader, PdfWriter
from pypdf.errors import PdfReadError
from sqlalchemy.orm import Session

from backend.app.core.config import get_settings
from backend.app.models.domain import Resume, StorageObject, StoredDocument, SystemError
from backend.app.services.resume_parser import parse_resume_text


MAX_OCR_BYTES = 10 * 1024 * 1024
MAX_OCR_PAGES = 8
LOW_CONFIDENCE_THRESHOLD = 0.7
STORAGE_DIR = Path("data/resumes")


@dataclass
class OcrResult:
    text: str
    confidence: float
    engine: str
    evidence: dict


class OcrEngine:
    name = "base"

    def extract_scanned_pdf(self, path: Path, *, languages: str = "chi_tra+eng", timeout_seconds: int = 60) -> OcrResult:
        raise NotImplementedError


class TesseractCliOcrEngine(OcrEngine):
    name = "tesseract_cli"

    def extract_scanned_pdf(self, path: Path, *, languages: str = "chi_tra+eng", timeout_seconds: int = 60) -> OcrResult:
        return extract_scanned_pdf_with_tesseract(path, languages=languages, timeout_seconds=timeout_seconds)


CommandRunner = Callable[..., subprocess.CompletedProcess]


def _run_command(args: list[str], *, timeout: int) -> subprocess.CompletedProcess:
    return subprocess.run(args, text=True, capture_output=True, timeout=timeout)


def _parse_tesseract_tsv(tsv: str) -> OcrResult:
    lines = [line for line in tsv.splitlines() if line.strip()]
    if len(lines) < 2:
        return OcrResult(text="", confidence=0.0, engine="tesseract_cli", evidence={"words": 0})
    header = lines[0].split("\t")
    try:
        conf_idx = header.index("conf")
        text_idx = header.index("text")
    except ValueError as exc:
        raise RuntimeError("Tesseract TSV output is missing conf/text columns.") from exc
    words: list[str] = []
    confidences: list[float] = []
    for line in lines[1:]:
        cols = line.split("\t")
        if len(cols) <= max(conf_idx, text_idx):
            continue
        word = cols[text_idx].strip()
        try:
            confidence = float(cols[conf_idx])
        except ValueError:
            confidence = -1
        if word and confidence >= 0:
            words.append(word)
            confidences.append(confidence)
    score = (sum(confidences) / len(confidences) / 100.0) if confidences else 0.0
    return OcrResult(text=" ".join(words), confidence=score, engine="tesseract_cli", evidence={"words": len(words), "mean_confidence": score})


def extract_scanned_pdf_with_tesseract(path: Path, *, languages: str = "chi_tra+eng", timeout_seconds: int = 60, runner: CommandRunner = _run_command) -> OcrResult:
    with tempfile.TemporaryDirectory() as tmp:
        image_prefix = Path(tmp) / "page"
        raster = runner(["pdftoppm", "-png", "-r", str(get_settings().ocr_raster_dpi), str(path), str(image_prefix)], timeout=timeout_seconds)
        if raster.returncode != 0:
            raise RuntimeError((raster.stderr or raster.stdout or "PDF rasterization failed")[:500])
        images = sorted(Path(tmp).glob("page-*.png"))
        if not images:
            raise RuntimeError("PDF rasterization produced no page images.")
        page_results: list[OcrResult] = []
        remaining_timeout = max(1, timeout_seconds)
        for image in images[:MAX_OCR_PAGES]:
            ocr = runner(["tesseract", str(image), "stdout", "-l", languages, "tsv"], timeout=remaining_timeout)
            if ocr.returncode != 0:
                raise RuntimeError((ocr.stderr or ocr.stdout or "Tesseract OCR failed")[:500])
            page_results.append(_parse_tesseract_tsv(ocr.stdout))
        text = "\n".join(result.text for result in page_results if result.text).strip()
        confidences = [result.confidence for result in page_results if result.confidence > 0]
        confidence = sum(confidences) / len(confidences) if confidences else 0.0
        return OcrResult(text=text, confidence=confidence, engine="tesseract_cli", evidence={"languages": languages, "pages_ocrd": len(page_results), "page_evidence": [item.evidence for item in page_results]})


class UnconfiguredOcrEngine(OcrEngine):
    name = "unconfigured"

    def extract_scanned_pdf(self, path: Path, *, languages: str = "chi_tra+eng", timeout_seconds: int = 60) -> OcrResult:
        raise RuntimeError("No raster OCR engine is configured. Install/configure Tesseract or a customer OCR provider before claiming scanned-PDF OCR is automated.")


def get_ocr_engine() -> OcrEngine:
    engine = get_settings().ocr_engine.lower().strip()
    if engine == "tesseract":
        return TesseractCliOcrEngine()
    return UnconfiguredOcrEngine()


def store_resume_document(db: Session, *, file_name: str, content: bytes, content_type: str, candidate_id: int | None = None) -> StoredDocument:
    if len(content) > MAX_OCR_BYTES:
        raise ValueError("Resume PDF exceeds the configured OCR size limit.")
    if not content.startswith(b"%PDF-") or b"%%EOF" not in content[-2048:]:
        raise ValueError("Uploaded file is not a structurally valid PDF.")
    digest = hashlib.sha256(content).hexdigest()
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in {".", "-", "_"} else "_" for ch in file_name)[:160] or "resume.pdf"
    version_id = uuid.uuid4().hex
    path = STORAGE_DIR / f"{digest}-{version_id}-{safe_name}"
    path.write_bytes(content)
    try:
        path.chmod(0o600)
    except OSError:
        pass
    storage_object = StorageObject(
        object_key=str(path),
        version_id=version_id,
        sha256=digest,
        byte_size=len(content),
        storage_provider="local_file",
        ref_count=1,
        evidence={"candidate_id": candidate_id, "file_name": file_name, "independent_upload_object": True},
    )
    db.add(storage_object)
    document = StoredDocument(
        candidate_id=candidate_id,
        file_name=file_name,
        storage_path=str(path),
        content_type=content_type,
        sha256=digest,
        byte_size=len(content),
        retention_policy={"live_db": "metadata only", "file": "restricted local object; purge through privacy deletion or external purge drill", "storage_object_version": version_id},
    )
    db.add(document)
    db.flush()
    return document


def _manual_review(db: Session, document: StoredDocument, *, feature: str, message: str, confidence: float = 0.0, evidence: dict | None = None) -> Resume:
    document.status = "Manual Review Required"
    db.add(SystemError(service="OCR", feature=feature, severity="Warning", message=message, impact="Resume is stored securely; HR review is required before parsing."))
    resume = Resume(
        candidate_id=document.candidate_id,
        source="ocr",
        file_name=document.file_name,
        raw_text="",
        parsed_json={"ocr_confidence": confidence, "document_id": document.id, "ocr_evidence": evidence or {}},
        status="Manual Review Required",
    )
    db.add(resume)
    return resume


def _write_subset_pdf(reader: PdfReader, page_indexes: list[int]) -> Path:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    tmp.close()
    writer = PdfWriter()
    for index in page_indexes:
        writer.add_page(reader.pages[index])
    with open(tmp.name, "wb") as handle:
        writer.write(handle)
    return Path(tmp.name)


def extract_pdf_text(db: Session, document: StoredDocument) -> Resume:
    path = Path(document.storage_path)
    try:
        reader = PdfReader(str(path))
    except PdfReadError as exc:
        return _manual_review(db, document, feature="pdf_read", message=f"PDF is encrypted, malformed, or unreadable: {exc}")
    if reader.is_encrypted:
        return _manual_review(db, document, feature="encrypted_pdf", message="Encrypted PDF cannot be parsed automatically.")
    document.page_count = len(reader.pages)
    if document.page_count <= 0:
        return _manual_review(db, document, feature="blank_pdf", message="PDF has no readable pages.")
    if document.page_count > MAX_OCR_PAGES:
        return _manual_review(db, document, feature="page_limit", message="PDF page count exceeds OCR limit.")
    page_texts: list[str] = []
    scanned_page_indexes: list[int] = []
    for index, page in enumerate(reader.pages):
        page_text = (page.extract_text() or "").strip()
        if page_text:
            page_texts.append(page_text)
        else:
            page_texts.append("")
            scanned_page_indexes.append(index)
    raw_text = "\n".join(part for part in page_texts if part).strip()
    if raw_text and not scanned_page_indexes:
        confidence = min(0.98, max(0.72, len(raw_text) / 1200))
        parsed = parse_resume_text(raw_text)
        document.status = "Text Extracted"
        resume = Resume(
            candidate_id=document.candidate_id,
            source="pdf_text",
            file_name=document.file_name,
            raw_text=raw_text,
            parsed_json={**parsed, "ocr_confidence": confidence, "document_id": document.id, "ocr_evidence": {"mode": "embedded_text", "pages": document.page_count}},
            status="Parsed",
        )
        db.add(resume)
        return resume
    if scanned_page_indexes:
        ocr_path = path
        if raw_text:
            ocr_path = _write_subset_pdf(reader, scanned_page_indexes)
        try:
            settings = get_settings()
            result = get_ocr_engine().extract_scanned_pdf(ocr_path, languages=settings.ocr_languages, timeout_seconds=int(settings.ocr_timeout_seconds))
        except Exception as exc:
            return _manual_review(db, document, feature="scanned_pdf_ocr_unverified", message=str(exc), evidence={"mode": "scanned_pdf", "engine": get_settings().ocr_engine or "unconfigured", "embedded_text_pages": document.page_count - len(scanned_page_indexes), "scanned_pages": len(scanned_page_indexes), "scanned_page_indexes": scanned_page_indexes})
        finally:
            if ocr_path != path:
                try:
                    ocr_path.unlink()
                except OSError:
                    pass
        combined_text = "\n".join(part for part in [raw_text, result.text] if part).strip()
        if result.confidence < LOW_CONFIDENCE_THRESHOLD or not combined_text:
            return _manual_review(db, document, feature="low_confidence", message="Scanned PDF OCR produced blank or low-confidence text.", confidence=result.confidence, evidence={**result.evidence, "embedded_text_pages": document.page_count - len(scanned_page_indexes), "scanned_pages": len(scanned_page_indexes), "scanned_page_indexes": scanned_page_indexes})
        parsed = parse_resume_text(combined_text)
        document.status = "OCR Parsed"
        resume = Resume(
            candidate_id=document.candidate_id,
            source="ocr",
            file_name=document.file_name,
            raw_text=combined_text,
            parsed_json={**parsed, "ocr_confidence": result.confidence, "document_id": document.id, "ocr_evidence": {"mode": "mixed_pdf" if raw_text else "scanned_pdf", "engine": result.engine, "embedded_text_pages": document.page_count - len(scanned_page_indexes), "scanned_pages": len(scanned_page_indexes), "scanned_page_indexes": scanned_page_indexes, **result.evidence}},
            status="Parsed",
        )
        db.add(resume)
        return resume
    return _manual_review(db, document, feature="blank_pdf", message="PDF produced no embedded text and no scanned pages were eligible for OCR.")
