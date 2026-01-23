"""The Deliverator - FastAPI application.

Takes the request from the client. Promotes body metadata to headers.
Delivers cleanly to the next stop. No transformation. Just delivery.

Pizza in thirty minutes or it's free.
"""

import json
import logging
import os

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Where we're delivering to
DOWNSTREAM_URL = os.environ.get("DOWNSTREAM_URL", "http://localhost:8080")

# The canaries that mark metadata blocks
# We look for DELIVERATOR first (new path), fall back to LOOM (legacy)
DELIVERATOR_CANARY = "DELIVERATOR_METADATA_UlVCQkVSRFVDSw"
LOOM_CANARY = "LOOM_METADATA_UlVCQkVSRFVDSw"

# Reusable HTTP client
_client: httpx.AsyncClient | None = None


async def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            base_url=DOWNSTREAM_URL,
            timeout=httpx.Timeout(300.0, connect=10.0),
        )
    return _client


app = FastAPI(
    title="The Deliverator",
    description="Pizza in thirty minutes or it's free.",
)


@app.on_event("shutdown")
async def shutdown():
    global _client
    if _client:
        await _client.aclose()
        _client = None


def extract_and_strip_metadata(body: dict) -> tuple[dict | None, dict]:
    """Find canary blocks, extract metadata from the LAST one found.

    Looks for DELIVERATOR_METADATA first (new path), then LOOM_METADATA (legacy).
    Returns (metadata, cleaned_body).
    Does NOT strip the block (yet) - just extracts metadata.
    """
    messages = body.get("messages", [])

    # Collect all metadata blocks, keeping track of which canary
    found_blocks = []  # List of (msg_idx, block_idx, metadata, canary_type)

    for msg_idx in range(len(messages)):
        msg = messages[msg_idx]
        if msg.get("role") != "user":
            continue

        content = msg.get("content")
        if not isinstance(content, list):
            continue

        for block_idx, block in enumerate(content):
            if not isinstance(block, dict) or block.get("type") != "text":
                continue

            text = block.get("text", "")

            # Check for DELIVERATOR canary (new path)
            if DELIVERATOR_CANARY in text:
                try:
                    start = text.index("{")
                    end = text.rindex("}") + 1
                    metadata = json.loads(text[start:end])
                    found_blocks.append((msg_idx, block_idx, metadata, "deliverator"))
                except (ValueError, json.JSONDecodeError) as e:
                    logger.warning(f"Deliverator: failed to parse DELIVERATOR metadata: {e}")
                continue

            # Check for LOOM canary (legacy path)
            if LOOM_CANARY in text:
                # Must be the actual metadata block, not a file diff mentioning it
                if "UserPromptSubmit hook additional context:" not in text:
                    continue
                try:
                    start = text.index("{")
                    end = text.rindex("}") + 1
                    metadata = json.loads(text[start:end])
                    found_blocks.append((msg_idx, block_idx, metadata, "loom"))
                except (ValueError, json.JSONDecodeError) as e:
                    logger.warning(f"Deliverator: failed to parse LOOM metadata: {e}")
                continue

    if not found_blocks:
        return None, body

    # Prefer DELIVERATOR blocks over LOOM blocks, take the last one of each type
    deliverator_blocks = [b for b in found_blocks if b[3] == "deliverator"]
    loom_blocks = [b for b in found_blocks if b[3] == "loom"]

    if deliverator_blocks:
        # Use the last DELIVERATOR block
        msg_idx, block_idx, metadata, canary_type = deliverator_blocks[-1]
        logger.info(f"Deliverator: extracted DELIVERATOR metadata, session={metadata.get('session_id', '?')[:8]}")
    elif loom_blocks:
        # Fall back to the last LOOM block
        msg_idx, block_idx, metadata, canary_type = loom_blocks[-1]
        logger.info(f"Deliverator: extracted LOOM metadata (legacy), session={metadata.get('session_id', '?')[:8]}")
    else:
        return None, body

    # For now, we do NOT strip the block - let the Loom handle that
    # This keeps the legacy path working while we transition

    return metadata, body


def filter_headers(headers: dict, skip_keys: set[str]) -> dict:
    """Filter headers, removing hop-by-hop and specified keys."""
    skip = {"host", "connection", "keep-alive", "transfer-encoding",
            "te", "trailers", "upgrade", "content-length"} | skip_keys
    return {k: v for k, v in headers.items() if k.lower() not in skip}


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def deliver(request: Request, path: str):
    """Deliver the request. Fast. Clean. No questions asked."""

    body_bytes = await request.body()
    headers = dict(request.headers)

    # Only process POST to messages endpoint
    is_messages = request.method == "POST" and "messages" in path
    metadata = None

    if is_messages and body_bytes:
        try:
            body = json.loads(body_bytes)
            metadata, body = extract_and_strip_metadata(body)

            if metadata:
                # Promote to headers
                if "traceparent" in metadata:
                    headers["traceparent"] = metadata["traceparent"]
                if "session_id" in metadata:
                    headers["x-session-id"] = metadata["session_id"]

                # Re-encode the cleaned body
                body_bytes = json.dumps(body).encode()

        except json.JSONDecodeError:
            pass  # Not JSON, just forward as-is

    # Forward to downstream
    client = await get_client()
    forward_headers = filter_headers(headers, {"traceparent", "x-session-id"} if not metadata else set())

    # Add our promoted headers
    if metadata:
        if "traceparent" in metadata:
            forward_headers["traceparent"] = metadata["traceparent"]
        if "session_id" in metadata:
            forward_headers["x-session-id"] = metadata["session_id"]

    upstream_response = await client.request(
        method=request.method,
        url=f"/{path}",
        headers=forward_headers,
        content=body_bytes,
        params=dict(request.query_params),
    )

    # Return response unchanged
    content_type = upstream_response.headers.get("content-type", "")
    response_headers = filter_headers(dict(upstream_response.headers), set())

    if "text/event-stream" in content_type:
        return StreamingResponse(
            upstream_response.aiter_bytes(),
            status_code=upstream_response.status_code,
            headers=response_headers,
            media_type="text/event-stream",
        )
    else:
        return Response(
            content=upstream_response.content,
            status_code=upstream_response.status_code,
            headers=response_headers,
        )
