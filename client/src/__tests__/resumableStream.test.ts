import { describe, expect, it, vi } from "vitest";
import {
  ResumableGeneration,
  applySequencedEvent,
  applySnapshot,
  applyTerminalEvent,
  createInitialGenerationState,
  type GenerationState,
  type TerminalEvent,
} from "../resumableStream";

function attachedState(): GenerationState {
  return {
    ...createInitialGenerationState(),
    phase: "attached",
    generationId: "generation-1",
    idempotencyKey: "request-1",
  };
}

function delta(text: string) {
  return { type: "text.delta", text };
}

function done(finalSeq: number): TerminalEvent {
  return {
    type: "completed",
    finalSeq,
    metadata: { model: "demo" },
  };
}

describe("sequenced event reducer", () => {
  it("counts and drops a duplicate event", () => {
    const once = applySequencedEvent(attachedState(), 1, delta("A"));
    const duplicate = applySequencedEvent(once, 1, delta("A"));

    expect(duplicate.text).toBe("A");
    expect(duplicate.cursor).toBe(1);
    expect(duplicate.duplicatesDropped).toBe(1);
  });

  it("requests a reconnect on a gap without moving the cursor", () => {
    const original = attachedState();
    const next = applySequencedEvent(original, 2, delta("B"));

    expect(next.text).toBe("");
    expect(next.cursor).toBe(0);
    expect(next.gapsSeen).toBe(1);
    expect(next.reconnectRequested).toBe(true);
    expect(next.phase).toBe("reconnecting");
  });

  it("applies a snapshot atomically and advances to its final cursor", () => {
    const withFirst = applySequencedEvent(attachedState(), 1, delta("A"));
    const next = applySnapshot(withFirst, {
      events: [
        { seq: 1, event: delta("A") },
        { seq: 2, event: delta("B") },
        { seq: 3, event: delta("C") },
      ],
    });

    expect(next.text).toBe("ABC");
    expect(next.cursor).toBe(3);
    expect(next.duplicatesDropped).toBe(1);
  });

  it("leaves text and cursor unchanged when a snapshot has a gap", () => {
    const withFirst = applySequencedEvent(attachedState(), 1, delta("A"));
    const next = applySnapshot(withFirst, {
      events: [
        { seq: 1, event: delta("A") },
        { seq: 3, event: delta("C") },
      ],
    });

    expect(next.text).toBe("A");
    expect(next.cursor).toBe(1);
    expect(next.gapsSeen).toBe(1);
    expect(next.reconnectRequested).toBe(true);
  });

  it("treats a repeated terminal event as a no-op", () => {
    const first = applyTerminalEvent(attachedState(), done(0));
    const second = applyTerminalEvent(first, done(0));

    expect(first.phase).toBe("completed");
    expect(second).toBe(first);
  });

  it("defers a terminal event until its final sequence arrives", () => {
    const deferred = applyTerminalEvent(attachedState(), done(2));

    expect(deferred.phase).toBe("reconnecting");
    expect(deferred.terminal).toBeNull();
    expect(deferred.deferredTerminal).toEqual(done(2));

    const withFirst = applySequencedEvent(deferred, 1, delta("A"));
    expect(withFirst.phase).toBe("reconnecting");

    const complete = applySequencedEvent(withFirst, 2, delta("B"));
    expect(complete.text).toBe("AB");
    expect(complete.cursor).toBe(2);
    expect(complete.phase).toBe("completed");
    expect(complete.terminal).toEqual(done(2));
  });

  it("converges to canonical text after duplicate and reordered attempts", () => {
    let state = attachedState();

    state = applySequencedEvent(state, 2, delta("B"));
    state = applySequencedEvent(state, 1, delta("A"));
    state = applySequencedEvent(state, 3, delta("C"));
    state = applySequencedEvent(state, 1, delta("A"));
    state = applySnapshot(state, {
      events: [
        { seq: 1, event: delta("A") },
        { seq: 2, event: delta("B") },
        { seq: 3, event: delta("C") },
      ],
    });
    state = applySequencedEvent(state, 2, delta("B"));
    state = applySequencedEvent(state, 3, delta("C"));

    expect(state.text).toBe("ABC");
    expect(state.cursor).toBe(3);
    expect(state.gapsSeen).toBe(2);
    expect(state.duplicatesDropped).toBe(4);
  });
});

describe("connection gate", () => {
  it("does not start a request after being disabled in the same turn", async () => {
    const fetchMock = vi.fn() as unknown as typeof fetch;
    const transport = new ResumableGeneration({ fetch: fetchMock });
    const started = transport.start({
      messages: [{ role: "user", content: "hello" }],
    });

    transport.setConnectionEnabled(false);
    await Promise.resolve();
    await Promise.resolve();

    expect(fetchMock).not.toHaveBeenCalled();
    transport.dispose();
    await expect(started).rejects.toMatchObject({ name: "AbortError" });
  });

  it("parks attach and cancel requests disabled in the same turn", async () => {
    const fetchMock = vi.fn() as unknown as typeof fetch;
    const transport = new ResumableGeneration({ fetch: fetchMock });

    transport.attach("generation-1", "request-1");
    transport.cancel();
    transport.setConnectionEnabled(false);
    await Promise.resolve();
    await Promise.resolve();

    expect(fetchMock).not.toHaveBeenCalled();
    transport.dispose();
  });

  it("fails instead of retrying when the generation is gone", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 404,
      body: null,
    });
    const transport = new ResumableGeneration({
      fetch: fetchMock as unknown as typeof fetch,
    });

    transport.attach("missing-generation", "request-1");
    await vi.waitFor(() => {
      expect(transport.getState().phase).toBe("failed");
    });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(transport.getState().terminalMetadata).toMatchObject({
      status: 404,
    });
    transport.dispose();
  });
});
