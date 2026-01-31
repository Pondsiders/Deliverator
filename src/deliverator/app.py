"""The Deliverator - FastAPI application.

Takes the request from the client. Promotes body metadata to headers.
Delivers cleanly to the next stop. No transformation. Just delivery.

Pizza in thirty minutes or it's free.
"""

import json
import logging
import os

# Suppress the harmless "Failed to detach context" warnings from OTel BEFORE importing
# These occur when spans cross async generator boundaries - expected behavior
logging.getLogger("opentelemetry.context").setLevel(logging.CRITICAL)

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
import httpx
import logfire

# Initialize Logfire
# Scrubbing disabled - too aggressive (redacts "session", "auth", etc.)
# Our logs are authenticated with 30-day retention; acceptable risk for debugging visibility
# console=False prevents stdout pollution that breaks trace propagation
logfire.configure(
    service_name="deliverator-greatloom",
    distributed_tracing=True,
    scrubbing=False,
    console=False,
    send_to_logfire="if-token-present",
)

# Instrument Python's standard logging library to flow to Logfire
# level=DEBUG for debugging pattern routing issues
logging.basicConfig(handlers=[logfire.LogfireLoggingHandler()], level=logging.DEBUG)

# instrument_httpx so outgoing requests propagate traceparent
logfire.instrument_httpx()

logger = logging.getLogger(__name__)

# Where we're delivering to
DOWNSTREAM_URL = os.environ.get("DOWNSTREAM_URL", "http://localhost:8080")

# The canaries that mark metadata blocks
# Priority: ALPHA (system prompt) > DELIVERATOR (message) > LOOM (legacy message)
ALPHA_CANARY = "ALPHA_METADATA_UlVCQkVSRFVDSw"
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

# NOTE: We intentionally do NOT use instrument_fastapi() here.
# We create manual spans inside the handler so we can attach the parent context
# from the traceparent we extract from the body. If we used instrument_fastapi(),
# it would create a root span before we have a chance to extract the traceparent.


@app.on_event("shutdown")
async def shutdown():
    global _client
    if _client:
        await _client.aclose()
        _client = None


def extract_and_strip_metadata(body: dict) -> tuple[dict | None, dict, str | None]:
    """Find canary blocks, extract metadata.

    Checks in priority order:
    1. ALPHA_METADATA in system prompt (new Duckpond path)
    2. DELIVERATOR_METADATA in messages (hook path)
    3. LOOM_METADATA in messages (legacy hook path)

    Returns (metadata, cleaned_body, canary_type).
    Does NOT strip the block (yet) - just extracts metadata.
    """
    # === Check system prompt first (ALPHA_METADATA - Duckpond path) ===
    system = body.get("system")
    if system:
        # System can be string or list of blocks
        if isinstance(system, str):
            system_text = system
        elif isinstance(system, list):
            system_text = " ".join(
                b.get("text", "") for b in system
                if isinstance(b, dict) and b.get("type") == "text"
            )
        else:
            system_text = ""

        if ALPHA_CANARY in system_text:
            try:
                start = system_text.index("{")
                end = system_text.rindex("}") + 1
                metadata = json.loads(system_text[start:end])
                logger.info(f"Deliverator: found ALPHA_METADATA in system prompt")
                return metadata, body, "alpha"
            except (ValueError, json.JSONDecodeError) as e:
                logger.warning(f"Deliverator: failed to parse ALPHA metadata: {e}")

    # === Fall back to messages (DELIVERATOR/LOOM path) ===
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
                # Must be the actual metadata block, not a file diff or code mentioning it
                # Real hook output starts with the hook name, e.g. "UserPromptSubmit hook additional context:"
                # We check that "hook additional context:" appears near the START of the text
                lower_text = text.lower()
                hook_marker_pos = lower_text.find("hook additional context:")
                if hook_marker_pos == -1 or hook_marker_pos > 100:
                    # Not found, or found too far in (probably code/comments)
                    continue
                try:
                    start = text.index("{")
                    end = text.rindex("}") + 1
                    metadata = json.loads(text[start:end])
                    # Verify it's actual metadata by checking for canary key
                    if "canary" not in metadata:
                        continue
                    found_blocks.append((msg_idx, block_idx, metadata, "deliverator"))
                except (ValueError, json.JSONDecodeError) as e:
                    logger.debug(f"Deliverator: skipping non-metadata block with canary string: {e}")
                continue

            # Check for LOOM canary (legacy path)
            if LOOM_CANARY in text:
                # Must be the actual metadata block, not a file diff or code mentioning it
                # Real hook output starts with the hook name
                lower_text = text.lower()
                hook_marker_pos = lower_text.find("hook additional context:")
                if hook_marker_pos == -1 or hook_marker_pos > 100:
                    continue
                try:
                    start = text.index("{")
                    end = text.rindex("}") + 1
                    metadata = json.loads(text[start:end])
                    # Verify it's actual metadata by checking for canary key
                    if "canary" not in metadata:
                        continue
                    found_blocks.append((msg_idx, block_idx, metadata, "loom"))
                except (ValueError, json.JSONDecodeError) as e:
                    logger.debug(f"Deliverator: skipping non-metadata block with canary string: {e}")
                continue

    if not found_blocks:
        return None, body, None

    # Prefer DELIVERATOR blocks over LOOM blocks, take the last one of each type
    deliverator_blocks = [b for b in found_blocks if b[3] == "deliverator"]
    loom_blocks = [b for b in found_blocks if b[3] == "loom"]

    if deliverator_blocks:
        # Use the last DELIVERATOR block
        msg_idx, block_idx, metadata, canary_type = deliverator_blocks[-1]
    elif loom_blocks:
        # Fall back to the last LOOM block
        msg_idx, block_idx, metadata, canary_type = loom_blocks[-1]
    else:
        return None, body, None

    # For now, we do NOT strip the block - let the Loom handle that
    # This keeps the legacy path working while we transition

    # Return canary_type so caller can log appropriately under span
    return metadata, body, canary_type


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
    session_id = None
    traceparent = None
    pattern = None

    canary_type = None
    if is_messages and body_bytes:
        try:
            body = json.loads(body_bytes)
            metadata, body, canary_type = extract_and_strip_metadata(body)

            if metadata:
                # Promote to headers
                if "traceparent" in metadata:
                    traceparent = metadata["traceparent"]
                    headers["traceparent"] = traceparent
                if "session_id" in metadata:
                    session_id = metadata["session_id"]
                    headers["x-session-id"] = session_id
                if "pattern" in metadata:
                    pattern = metadata["pattern"]
                    headers["x-loom-pattern"] = pattern
                if "machine" in metadata and isinstance(metadata["machine"], dict):
                    # Extract FQDN from machine info for the Loom
                    fqdn = metadata["machine"].get("fqdn", "")
                    if fqdn:
                        headers["x-machine-name"] = fqdn

                # Re-encode the cleaned body
                body_bytes = json.dumps(body).encode()

        except json.JSONDecodeError:
            pass  # Not JSON, just forward as-is

    # === Set up tracing ===
    # Attach parent context from traceparent if present
    # NOTE: logfire.attach_context() expects a simple dict like {"traceparent": "00-..."}
    # NOT an OTel context object from extract(). This is the Logfire way.
    if traceparent:
        ctx_manager = logfire.attach_context({"traceparent": traceparent})
        ctx_manager.__enter__()
    else:
        ctx_manager = None

    # Create span for delivery
    span_name = f"deliver: {request.method} /{path}"

    span_attrs = {"endpoint": f"/{path}"}
    if session_id:
        span_attrs["session.id"] = session_id[:8]

    # Add pattern as Logfire tag (not standard OTel, but useful for filtering)
    if pattern:
        span_attrs["_tags"] = [pattern]

    span = logfire.span(span_name, **span_attrs)
    span.__enter__()

    # Log metadata extraction results (now under the span)
    if canary_type == "alpha":
        logger.info(f"Deliverator: extracted ALPHA metadata (system prompt), session={session_id[:8] if session_id else '?'}")
    elif canary_type == "deliverator":
        logger.info(f"Deliverator: extracted DELIVERATOR metadata, session={session_id[:8] if session_id else '?'}")
    elif canary_type == "loom":
        logger.info(f"Deliverator: extracted LOOM metadata (legacy), session={session_id[:8] if session_id else '?'}")

    if metadata:
        logger.info(f"Deliverator: pattern={metadata.get('pattern', 'none')}")
        logger.debug(f"Deliverator: full metadata = {metadata}")
        logfire.info(
            "Delivering request",
            session=session_id[:8] if session_id else "none",
            has_traceparent=traceparent is not None,
        )
    else:
        logger.debug("Deliverator: no metadata found in request")

    try:
        # Forward to downstream
        client = await get_client()
        forward_headers = filter_headers(headers, {"traceparent", "x-session-id"} if not metadata else set())

        # Add our promoted headers
        if metadata:
            if traceparent:
                forward_headers["traceparent"] = traceparent
            if session_id:
                forward_headers["x-session-id"] = session_id

        # Log outgoing headers for debugging pattern routing
        logger.debug(f"Deliverator: outgoing headers = {forward_headers}")

        # Check if this is a streaming request
        is_streaming_request = False
        if is_messages and body_bytes:
            try:
                parsed = json.loads(body_bytes)
                is_streaming_request = parsed.get("stream", False)
            except json.JSONDecodeError:
                pass

        if is_streaming_request:
            # === Streaming request - use true streaming ===
            # Use client.stream() to avoid buffering the entire response
            captured_span = span
            captured_ctx_manager = ctx_manager

            async def stream_with_span():
                try:
                    async with client.stream(
                        method=request.method,
                        url=f"/{path}",
                        headers=forward_headers,
                        content=body_bytes,
                        params=dict(request.query_params),
                    ) as upstream_response:
                        captured_span.set_attribute("http.status_code", upstream_response.status_code)

                        async for chunk in upstream_response.aiter_bytes():
                            yield chunk
                finally:
                    # End the span after streaming completes
                    try:
                        captured_span.__exit__(None, None, None)
                    except ValueError:
                        pass  # Cross-context detach, harmless
                    if captured_ctx_manager:
                        try:
                            captured_ctx_manager.__exit__(None, None, None)
                        except ValueError:
                            pass  # Cross-context detach, harmless

            return StreamingResponse(
                stream_with_span(),
                status_code=200,  # Actual status comes from upstream
                headers={
                    "content-type": "text/event-stream",
                    "cache-control": "no-cache",
                    "connection": "keep-alive",
                    "x-accel-buffering": "no",  # Disable nginx buffering
                },
                media_type="text/event-stream",
            )

        else:
            # === Non-streaming request - buffering is fine ===
            upstream_response = await client.request(
                method=request.method,
                url=f"/{path}",
                headers=forward_headers,
                content=body_bytes,
                params=dict(request.query_params),
            )

            content_type = upstream_response.headers.get("content-type", "")
            response_headers = filter_headers(dict(upstream_response.headers), set())
            status_code = upstream_response.status_code

            span.set_attribute("http.status_code", status_code)

            # Non-streaming - close span immediately
            span.__exit__(None, None, None)
            if ctx_manager:
                ctx_manager.__exit__(None, None, None)

            return Response(
                content=upstream_response.content,
                status_code=status_code,
                headers=response_headers,
            )

    except Exception as e:
        span.record_exception(e)
        span.set_level("error")
        span.__exit__(None, None, None)
        if ctx_manager:
            ctx_manager.__exit__(None, None, None)
        logfire.error("Deliverator error", error=str(e))
        raise
