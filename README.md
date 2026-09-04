# LLM Token Streaming with Disconnect Tolerance

Production LLM applications need low-latency token delivery without making generation reliability depend on one long-lived HTTP request. In a conventional stream, a broken browser connection cannot request only the missing suffix. Regenerating repeats the full latency and output-token cost, and sampling may produce a different answer.

This project makes token streaming disconnect-tolerant by separating generation from delivery. The server runs each generation in an independent task and records every provider delta in an ordered PostgreSQL log. A browser can disconnect, reload, or attach from another tab, then resume from its last sequence without starting another provider request.

https://github.com/user-attachments/assets/0cf6a09a-cc3f-40c0-84cf-d819a0f2aa5a

**Example of this project's fault tolerance under repeated simulated client-side connection failures.**

## Overview

The system has three independent parts:

- a generator task consumes one provider stream and writes typed events;
- PostgreSQL stores the durable sequence for each generation;
- each attached browser reads that sequence through Server-Sent Events.

The browser includes a **Turn off connection** control. It aborts the real streaming fetch and parks the reconnect loop. Nothing on the server knows this control exists. The generator keeps writing while no reader is attached.

Turning the connection back on attaches with the last in-memory cursor. A backlog of at most `SNAPSHOT_THRESHOLD` rows, 50 by default, arrives as individual chunk events. A larger backlog arrives in one snapshot, so the answer reaches the live edge in one render.

Generation uses OpenRouter chat completions. Set `OPENROUTER_API_KEY` in `.env`.

## Delivery guarantees

The server provides at-least-once delivery. A reconnect may race with the final event from the previous connection, so the same sequence can arrive twice.

The client applies each sequence exactly once:

```text
seq <= cursor      drop duplicate
seq == cursor + 1  append event and advance cursor
seq > cursor + 1   preserve state and reconnect
```

A snapshot follows the same rule. The reducer validates its complete new suffix before changing the text or cursor. A malformed or gapped snapshot therefore changes neither the text nor the cursor.

Each generation has one writer. That writer assigns dense sequence numbers starting at one, so any client that receives the complete sequence assembles the same text in the same order.

## How generation stays alive

`POST /generations` inserts a generation, starts an asyncio task, and returns its ID without waiting for generation to finish:

```http
POST /generations
Idempotency-Key: <uuid>
```

```json
{"generation_id":"<uuid>"}
```

The browser uses that ID in a separate streaming request:

```http
GET /generations/{id}/events?after=N
Accept: text/event-stream
```

The request that started generation no longer owns the provider stream. The generator task consumes the provider, assigns sequence numbers, and writes small batches to PostgreSQL. Only after a batch commits does it publish a wakeup for attached readers.

The write loop waits on three conditions: the next provider delta, cancellation, or the flush deadline. This matters when a provider pauses. A loop driven only by the next delta could leave buffered text uncommitted and ignore a stop request for the length of that pause.

Terminal status is written after the final chunk commit. A reader that sees `completed` can therefore trust that every row through `final_seq` is available.

## Catch-up and live tailing

A reader subscribes to the in-process wakeup hub before its first database read. This closes the race where a commit could occur between catch-up and subscription.

Wakeups contain no stream data. They only tell a reader to run another indexed query:

```sql
SELECT seq, event
FROM chunks
WHERE generation_id = $1 AND seq > $2
ORDER BY seq;
```

Duplicate and missed wakeups are harmless because the cursor query remains authoritative. Idle readers hold no database connection. A heartbeat comment at the configured interval, 15 seconds by default, keeps proxies from treating a quiet stream as abandoned.

When a reader observes terminal status, it still checks that its cursor has reached `final_seq`. It sends `done`, `cancelled`, `failed`, or `interrupted` only after all preceding rows have been delivered.

## Idempotent start and durable stop

The client creates a UUID before starting and sends it as `Idempotency-Key`. The server inserts with a unique constraint. If the response disappears after the insert, the client retries the same request and receives the existing generation ID. The pending key and request remain in local storage until that response arrives, so a reload during the ambiguous window retries the same start. A transient start failure cannot create a second provider stream.

Stop is also retry-safe. The cancel endpoint first stores `cancel_requested = TRUE`, then signals the local generator task. A stop clicked while the demo connection is disabled waits in the client transport and is sent when the connection returns.

The generator closes the provider stream, flushes buffered text, and records `cancelled` with the last committed sequence. Every attached tab emits that terminal event only after reading all preceding chunks.

## Server restart boundary

Provider streams are not resumable after the Python process exits. On startup, the server marks every row left in `running` as `interrupted` and sets its final sequence to the last committed chunk.

The UI preserves that partial answer and states that generation was interrupted. It does not silently regenerate or invent a continuation.

Run the application with one uvicorn worker. Task ownership and wakeups are process-local. A multi-worker deployment needs a database lease for writer ownership and `LISTEN/NOTIFY` or an equivalent shared wakeup channel.

## API

Start a generation:

```http
POST /generations
Idempotency-Key: <uuid>
Content-Type: application/json

{"model":"optional-model","messages":[{"role":"user","content":"Explain WAL."}]}
```

```json
{"generation_id":"<uuid>"}
```

Attach or resume:

```http
GET /generations/{id}/events?after=183
Accept: text/event-stream
```

The stream emits `chunk`, `snapshot`, `done`, `cancelled`, `failed`, or `interrupted`. `Last-Event-ID` is also accepted. A cursor ahead of the durable log returns 409, and the client reconstructs from zero.

The remaining endpoints are:

```text
GET  /generations/{id}          current state and assembled text
POST /generations/{id}/cancel   durable idempotent cancellation
GET  /health                    database connectivity and provider name
```

## Provider

`OpenRouterProvider` streams chat completions from OpenRouter and maps each content delta into the internal `text.delta` event. Usage is stored from the terminal chunk when OpenRouter includes it. The API key remains in the server process. `MODEL` and `OPENROUTER_BASE_URL` are optional.

## Testing

The client parser is tested with every byte split of a sample SSE stream, including a split inside a multibyte character. Reducer tests cover duplicates, gaps, snapshots, repeated terminal events, and terminal events received before their final chunk.

Backend tests cover flush batching, cancellation during a provider pause, shutdown behavior, snapshots, terminal ordering, task ownership, provider mapping, configuration, idempotent start, and CORS.

Run both suites:

```text
python -m pytest server/tests
npm --prefix client test
```

## Code map

`server/generator.py` owns the write path. `server/reader.py` owns catch-up and live tailing. `server/database.py` contains the durable operations, while `server/providers.py` contains the OpenRouter adapter.

`client/src/resumableStream.ts` owns cursor reduction, retries, the watchdog, and the connection gate. `client/src/sseParser.ts` handles arbitrary streaming byte boundaries. React state and presentation remain in `useGeneration.ts` and `App.tsx`.

## Running the project

The project requires Python 3.11 or newer, Node.js 20 or newer, and Docker with Compose.

Copy `.env.example` to `.env` and set `OPENROUTER_API_KEY`. The server loads that file from the repository root at startup.

Install dependencies:

```text
python -m pip install -r server/requirements.txt
npm --prefix client install
```

Start PostgreSQL:

```text
docker compose up -d postgres
```

Run the API from the repository root:

```text
python -m uvicorn server.app:app --reload --port 8000
```

Run the client in another terminal:

```text
npm --prefix client run dev
```
