"""Ingest router — ``POST /api/logs`` and ``POST /api/logs/bulk``.

Both endpoints are thin shells around
:class:`~src.index.inverted_index.InvertedIndex`: the handler reaches
for the index on ``app.state`` (stashed by :func:`src.main.build_app`),
awaits the append-only write, and echoes back the assigned ``doc_id``
range plus the post-write ``index_version`` so callers can pair their
batch with what landed in the corpus.

Both return ``202 Accepted`` rather than ``201`` because the write is
already applied to the in-process index by the time the response goes
out — the ``202`` says "this has been admitted", which matches the
single-node, synchronous-write reality of the service. Clients that
need a stronger guarantee should pair this with the ``index_version``
echoed in the response.
"""

from fastapi import APIRouter, Request, status

from src.models import LogAckResponse, LogBulkRequest, LogEntry


router = APIRouter(prefix="/api/logs", tags=["ingest"])


@router.post(
    "",
    response_model=LogAckResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_one(entry: LogEntry, request: Request) -> LogAckResponse:
    """Admit a single log entry into the inverted index.

    The pydantic :class:`~src.models.LogEntry` schema enforces the
    non-empty message + valid level + timestamp contract, so by the
    time we reach the handler body the payload is already sane and
    the index just has to tokenize + write.
    """
    index = request.app.state.index
    doc_id = await index.add(entry)
    return LogAckResponse(
        accepted=1,
        first_doc_id=doc_id,
        last_doc_id=doc_id,
        index_version=index.version,
    )


@router.post(
    "/bulk",
    response_model=LogAckResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_bulk(
    payload: LogBulkRequest, request: Request
) -> LogAckResponse:
    """Admit a batch of entries in one writer-lock acquisition.

    :class:`~src.models.LogBulkRequest` enforces ``1 <= len <= 10000``
    at the schema layer, so ``doc_ids[0]``/``doc_ids[-1]`` below are
    always safe — the list can never be empty.
    """
    index = request.app.state.index
    doc_ids = await index.add_bulk(payload.entries)
    return LogAckResponse(
        accepted=len(doc_ids),
        first_doc_id=doc_ids[0],
        last_doc_id=doc_ids[-1],
        index_version=index.version,
    )
