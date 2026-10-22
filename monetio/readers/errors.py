"""Failure taxonomy for the reader package.

The rule this module exists to enforce: **a failed read must never look like a
successful one.** For a toolkit used in model-and-observation verification a
quiet failure is worse than a loud one, because it enters the science as "no
data" rather than as "failed read".

Every failure raised by the reader package carries exactly one category from the
closed taxonomy below, names the specific input involved, and names the
underlying cause. Where the platform already raised something meaningful — a
``FileNotFoundError`` for a missing file, a ``PermissionError`` for an unreadable
one — the original exception type is *preserved* rather than flattened into a
generic error, so a caller's ``except`` clause keeps working; the category is
attached alongside it.

Two halves live here:

``ReaderError`` and its subclasses
    The named categories. Subclasses exist so a call site can raise the right
    one without composing the message by hand.

``Tolerance`` / ``ToleranceRecord`` / ``SkippedInput``
    The caller's explicit request to accept a load with some inputs skipped, and
    the record of what was actually skipped. Tolerance is never implied and never
    the default: a load without it fails on the first bad input.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

__all__ = [
    "FAILURE_CATEGORIES",
    "ReaderError",
    "InputUnreadableError",
    "MalformedInputError",
    "ContentInvalidError",
    "SectionUnavailableError",
    "OptionUnsupportedError",
    "CapabilityUnavailableError",
    "RetrievalFailedError",
    "CredentialDeniedError",
    "MetadataMismatchError",
    "DependencyMissingError",
    "TransformNotAppliedError",
    "annotate",
    "attach_category",
    "classify_transport_error",
    "infer_category",
    "is_absent_section",
    "Tolerance",
    "ToleranceRecord",
    "SkippedInput",
    "format_tolerance_record",
]

ToleranceScope = Literal["input", "section"]

# The closed set from the Phase 2 data model. Every failure carries exactly one.
FAILURE_CATEGORIES = frozenset(
    {
        "not_found",
        "permission_denied",
        "credential_rejected",
        "retrieval_failed",
        "content_invalid",
        "malformed",
        "section_unavailable",
        "option_unsupported",
        "capability_unavailable",
        "metadata_mismatch",
        "dependency_missing",
    }
)

# How many inputs a single message names before indicating that more were omitted.
_MAX_NAMED_INPUTS = 5


def _preview(locations) -> str:
    """Render up to five locations, indicating omission rather than truncating."""
    items = [str(location) for location in locations]
    if not items:
        return ""
    head = items[:_MAX_NAMED_INPUTS]
    text = ", ".join(head)
    if len(items) > _MAX_NAMED_INPUTS:
        text += f", and {len(items) - _MAX_NAMED_INPUTS} more"
    return text


class ReaderError(Exception):
    """Base class for every failure raised by the reader package.

    Parameters
    ----------
    message
        Human-readable description of what went wrong.
    locations
        The specific inputs involved (paths, URLs, object-store keys). Named in
        the rendered message so a caller can tell *which* file failed.
    cause
        The underlying error message, kept verbatim rather than paraphrased.
    category
        One of :data:`FAILURE_CATEGORIES`. Subclasses set this; it is rarely
        passed directly.
    """

    category: str = "malformed"

    def __init__(self, message: str = "", *locations, cause: str | None = None, category=None):
        self.message = message
        self.locations = [str(location) for location in locations]
        self.cause = cause
        if category is not None:
            if category not in FAILURE_CATEGORIES:
                raise ValueError(f"unknown failure category {category!r}")
            self.category = category
        # Keep the rendered message in ``args`` so the exception round-trips
        # through pickling (a worker-side failure re-raised on a client keeps
        # its text) while ``__str__`` renders from live state.
        super().__init__(self._render())

    def _render(self) -> str:
        parts = [self.message] if self.message else []
        if self.locations:
            parts.append(f"[inputs: {_preview(self.locations)}]")
        if self.cause:
            parts.append(f"cause: {self.cause}")
        return " ".join(parts) if parts else self.category

    def __str__(self) -> str:
        # Rendered from current state, so a location added after construction
        # (annotating an error with the caller's own inputs) shows up in the
        # message rather than only in the structured attribute.
        return self._render()


class InputUnreadableError(ReaderError):
    """An input exists but could not be read at all."""

    category = "malformed"


class MalformedInputError(ReaderError, ValueError):
    """The input is the right kind of data but cannot be parsed.

    Subclasses :class:`ValueError` because that is what the parsing layers
    (xarray, pandas, the format readers) raise for bad content, so existing
    callers that catch ``ValueError`` keep working.
    """

    category = "malformed"

    def __init__(self, location, reason, *, message=None):
        super().__init__(
            message or "could not parse input",
            location,
            cause=str(reason) if reason else None,
        )


class ContentInvalidError(ReaderError):
    """A payload was retrieved successfully but is not the data it claims to be.

    Distinct from :class:`RetrievalFailedError`: the bytes arrived, so retrying
    the request will not help — the location or the request was wrong.
    """

    category = "content_invalid"


class SectionUnavailableError(ReaderError):
    """One named section of a multi-section input could not be opened."""

    category = "section_unavailable"

    def __init__(self, location, section, cause=None, *, message=None):
        self.section = str(section)
        text = message or f"could not open section {self.section!r}"
        super().__init__(text, location, cause=cause)


def is_absent_section(exc: BaseException) -> bool:
    """True when ``exc`` means the named section is simply not in the input.

    A multi-section reader must tell apart a section that does not exist in this
    particular file (legitimate — record it and carry on) from a section that
    exists but could not be read (a failure — raise). The storage engines report
    the first as a "group not found" style error and the second as a file-level
    open failure, so the distinction is made on the message.
    """
    text = str(exc).lower()
    return any(
        marker in text for marker in ("group not found", "section not found", "no such group")
    )


class OptionUnsupportedError(ReaderError, TypeError):
    """The source cannot honor a requested option.

    Subclasses :class:`TypeError` because an unsupported keyword surfaces that
    way today (from the storage backends); the point of raising it here is that
    the error names the *source and the option* instead of a backend engine.
    """

    category = "option_unsupported"

    def __init__(self, option, source, *, cause=None):
        self.option = str(option)
        self.source = str(source)
        super().__init__(f"source {source!r} does not support option {option!r}", cause=cause)


class CapabilityUnavailableError(ReaderError, NotImplementedError):
    """The source does not support a requested scale behavior.

    Subclasses :class:`NotImplementedError` to preserve the refusal shape the
    conformance suite already asserts for sources that declare a capability
    ``False``. The recorded reason is quoted verbatim so the caller learns *why*.
    """

    category = "capability_unavailable"

    def __init__(self, source, option, reason, *, cause=None):
        self.source = str(source)
        self.option = str(option)
        self.reason = str(reason)
        # The manifest field is spelled "deferred" for option "defer".
        capability = {"defer": "deferred"}.get(option.lower(), f"{option.lower()}ed")
        super().__init__(
            f"Source {source!r} does not support {option} loading "
            f"(capabilities.{capability}=False): {reason}. "
            f"Load it without {option}=True.",
            cause=cause,
        )


class RetrievalFailedError(ReaderError):
    """A transport failure: timeout, connection error, or exhausted retries."""

    category = "retrieval_failed"

    def __init__(self, location, cause=None, *, attempts=None):
        self.attempts = attempts
        text = "retrieval failed"
        if attempts is not None:
            text += f" after {attempts} attempt(s)"
        super().__init__(text, location, cause=cause)


class CredentialDeniedError(ReaderError):
    """The service rejected authentication or authorization.

    Never reported as a missing file: a caller must be able to tell "fix your
    credentials" from "this path does not exist".
    """

    category = "credential_rejected"

    def __init__(self, location, cause=None, *, service=None):
        self.service = service
        text = "access denied by the service"
        if service:
            text = f"the service {service!r} rejected the credentials"
        super().__init__(text, location, cause=cause)


class MetadataMismatchError(ReaderError):
    """Declared result metadata disagrees with the actual data."""

    category = "metadata_mismatch"


class DependencyMissingError(ReaderError, ImportError):
    """An optional dependency required by this code path is absent.

    Subclasses :class:`ImportError` so the conventional catch keeps working. The
    message names the dependency and how to install it.
    """

    category = "dependency_missing"

    def __init__(self, dependency, install_hint, *, location=None):
        self.dependency = str(dependency)
        text = f"optional dependency {dependency!r} is not installed. {install_hint}"
        super().__init__(text, *((location,) if location else ()))


class TransformNotAppliedError(ReaderError):
    """A shared transformation could not perform its stated job.

    Returning the input unchanged and letting the caller believe the
    transformation happened is the failure mode this class exists to prevent.
    """

    category = "malformed"

    def __init__(self, transform, reason, *, location=None):
        self.transform = str(transform)
        text = f"{transform} could not be applied: {reason}"
        super().__init__(text, *((location,) if location else ()), cause=reason)


# ---------------------------------------------------------------------------
# Category-preserving annotation
# ---------------------------------------------------------------------------


def attach_category(exc: BaseException, category: str) -> BaseException:
    """Attach ``category`` to ``exc`` without changing its type.

    Built-in exceptions accept attribute assignment, which lets a
    ``FileNotFoundError`` keep being a ``FileNotFoundError`` while still
    reporting a taxonomy category to callers that ask for one.
    """
    if category not in FAILURE_CATEGORIES:
        raise ValueError(f"unknown failure category {category!r}")
    try:
        exc.category = category
    except (AttributeError, TypeError):  # pragma: no cover - slotted exception types
        pass
    return exc


# Exception types whose identity already tells the caller what went wrong.
_CATEGORY_BY_TYPE = (
    (FileNotFoundError, "not_found"),
    (PermissionError, "permission_denied"),
    (TimeoutError, "retrieval_failed"),
    (ConnectionError, "retrieval_failed"),
)


def infer_category(exc: BaseException) -> str:
    """Best-effort taxonomy category for an exception raised elsewhere.

    Used when annotating a platform error so the category reflects the original
    failure rather than defaulting to ``malformed``.
    """
    for types, category in _CATEGORY_BY_TYPE:
        if isinstance(exc, types):
            return category
    if isinstance(exc, ReaderError):
        return exc.category
    return "malformed"


# HTTP status codes that mean "the service rejected our authentication", which
# must never be reported as a missing file.
_AUTH_STATUS = {401, 403}


def classify_transport_error(exc: BaseException, location, *, service=None):
    """Map a network/storage exception to the right failure category.

    Retrieval failures (connection, timeout, exhausted retries) are retryable;
    an authentication rejection is not, and a caller must be able to tell them
    apart from a genuinely missing input. The original exception is preserved as
    the cause in every branch. Returns the annotated original for anything this
    cannot confidently categorize, so a caller never loses the underlying error.
    """
    # requests: HTTPError carries a response with a status code.
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if status in _AUTH_STATUS:
        return CredentialDeniedError(location, cause=str(exc), service=service)
    # botocore/fsspec credential failures.
    name = type(exc).__name__
    if name in {"NoCredentialsError", "PartialCredentialsError", "CredentialRetrievalError"} or (
        isinstance(response, dict)
        and response.get("Error", {}).get("Code", "")
        in {"AccessDenied", "InvalidAccessKeyId", "ExpiredToken", "SignatureDoesNotMatch"}
    ):
        return CredentialDeniedError(location, cause=str(exc), service=service)
    # Any other HTTP error status (404, 5xx, ...): the payload was never
    # served, which is a retrieval problem — not a parse problem, and not a
    # local missing file.
    if isinstance(status, int) and status >= 400:
        return RetrievalFailedError(location, cause=str(exc))
    try:
        import requests

        if isinstance(exc, requests.exceptions.ConnectionError | requests.exceptions.Timeout):
            return RetrievalFailedError(location, cause=str(exc))
    except ImportError:  # pragma: no cover - requests is a hard dependency for these paths
        pass
    if isinstance(exc, TimeoutError | ConnectionError):
        return RetrievalFailedError(location, cause=str(exc))
    return annotate(exc, location)


def annotate(exc: BaseException, *locations, category: str | None = None) -> BaseException:
    """Return a same-typed copy of ``exc`` whose message names the given inputs.

    An error must identify the specific file(s) involved while keeping its
    original exception type, so a caller can still distinguish "file not found"
    from "permission denied" from "malformed content". Falls back to the
    untouched exception when the type cannot be reconstructed, and attaches a
    taxonomy category either way.
    """
    if isinstance(exc, ReaderError):
        # Already carries locations and a category; re-wrapping would lose the
        # structured message (the subclass constructors take (location, ...),
        # not a single message string).
        for location in locations:
            if str(location) not in exc.locations:
                exc.locations.append(str(location))
        return exc
    target_category = category or getattr(exc, "category", None) or infer_category(exc)
    preview = _preview(locations)
    annotated: BaseException
    if preview:
        try:
            annotated = type(exc)(f"{exc} [inputs: {preview}]")
        except Exception:  # noqa: BLE001 - exotic constructors; keep the original
            annotated = exc
    else:
        annotated = exc
    attach_category(annotated, target_category)
    if not getattr(annotated, "locations", None) and preview:
        try:
            annotated.locations = [str(location) for location in locations]
        except (AttributeError, TypeError):  # pragma: no cover
            pass
    return annotated


# ---------------------------------------------------------------------------
# Tolerance: explicit caller request, and the record of what it skipped
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Tolerance:
    """A caller's explicit instruction to accept a load with inputs skipped.

    Never implied and never the default. ``scope`` selects whether tolerance
    applies to whole inputs (one bad file of many) or to sections within a single
    multi-section input.
    """

    enabled: bool = False
    scope: ToleranceScope = "input"

    @classmethod
    def parse(cls, value) -> Tolerance | None:
        """Normalize a caller-supplied ``tolerate=`` value.

        Accepts ``None``/``False`` (no tolerance), ``True`` (input scope), or a
        scope string. Anything else is rejected rather than silently ignored.
        """
        if value is None or value is False:
            return None
        if value is True:
            return cls(enabled=True, scope="input")
        if isinstance(value, str):
            if value not in ("input", "section"):
                raise OptionUnsupportedError(
                    "tolerate", f"unknown scope {value!r} (expected 'input' or 'section')"
                )
            return cls(enabled=True, scope=value)
        if isinstance(value, Tolerance):
            return value if value.enabled else None
        raise OptionUnsupportedError("tolerate", f"unsupported value {value!r}")


@dataclass(frozen=True)
class SkippedInput:
    """One input or section that a tolerated load skipped."""

    location: str
    reason: str
    category: str


@dataclass(frozen=True)
class ToleranceRecord:
    """What a tolerated load actually skipped, for attachment to provenance.

    A skip that is not recorded is a contract failure: the record is what makes a
    tolerated load auditable, so a scientist can see that 3 of 31 daily files
    were skipped and why instead of inferring it from a row count.
    """

    skipped: tuple[SkippedInput, ...] = ()
    included_sections: tuple[str, ...] = ()
    discarded_rows: int = 0

    @property
    def empty(self) -> bool:
        return not self.skipped and not self.included_sections and not self.discarded_rows

    def record_skip(self, location, reason, category) -> ToleranceRecord:
        return ToleranceRecord(
            skipped=self.skipped + (SkippedInput(str(location), str(reason), category),),
            included_sections=self.included_sections,
            discarded_rows=self.discarded_rows,
        )

    def with_sections(self, *sections) -> ToleranceRecord:
        return ToleranceRecord(
            skipped=self.skipped,
            included_sections=tuple(str(section) for section in sections),
            discarded_rows=self.discarded_rows,
        )

    def with_discarded_rows(self, count: int) -> ToleranceRecord:
        return ToleranceRecord(
            skipped=self.skipped,
            included_sections=self.included_sections,
            discarded_rows=int(count),
        )


def format_tolerance_record(record: ToleranceRecord, *, requested: bool = False) -> str | None:
    """Render a :class:`ToleranceRecord` as one provenance line.

    Returns ``None`` when there is nothing to record, so callers can append
    unconditionally. The discarded-row count is emitted even when zero, because
    "nothing was discarded" is the information a mass-balance check needs.
    """
    if record.empty and not requested:
        return None
    parts = []
    if record.skipped:
        listing = "; ".join(
            f"{item.location} ({item.category}: {item.reason})" for item in record.skipped
        )
        parts.append(f"skipped {len(record.skipped)} input(s) under tolerance: {listing}")
    if record.included_sections:
        parts.append(f"included sections: {', '.join(record.included_sections)}")
    parts.append(f"discarded {record.discarded_rows} record(s) with missing location")
    return " | ".join(parts)
