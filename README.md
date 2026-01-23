# The Deliverator

> Pizza in thirty minutes or it's free.

The first proxy in the pipeline. Takes the request from the client, promotes body metadata to HTTP headers, strips the canary block, delivers cleanly to the Loom.

Named for Hiro Protagonist's gig in *Snow Crash*—delivering pizza for the Mafia at 140mph. The request comes in hot, the Deliverator gets it where it needs to go.

## What It Does

1. Receives request from Claude Agent SDK
2. Finds the `LOOM_METADATA_*` canary block in the message body
3. Extracts `traceparent` and `session_id`
4. Strips the canary block from the body
5. Promotes metadata to HTTP headers:
   - `traceparent: 00-{trace_id}-{span_id}-01`
   - `x-session-id: {session_id}`
6. Forwards cleaned request to downstream (the Loom)
7. Returns response unchanged

## The Pipeline

```
Client → Hook (injects body metadata)
           ↓
     **Deliverator** (promotes to headers, strips canary)
           ↓
     **Great Loom** (transforms: system prompt, memories)
           ↓
     **Argonath** (observes: perfect LLM traces)
           ↓
       Anthropic
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DOWNSTREAM_URL` | `http://localhost:8080` | Where to deliver (the Loom) |

## Running

```bash
docker compose up -d
```

Point your Claude Code at `http://primer:8079` and let it rip.

---

*"The Deliverator belongs to an elite order, a hallowed subcategory."*
— Neal Stephenson, *Snow Crash*
