import { SseParser, type SseFrame } from "./sseParser";

export type GenerationPhase =
  | "idle"
  | "starting"
  | "attached"
  | "reconnecting"
  | "completed"
  | "cancelled"
  | "failed"
  | "interrupted";

export type TerminalPhase =
  | "completed"
  | "cancelled"
  | "failed"
  | "interrupted";

export interface TerminalEvent {
  type: TerminalPhase;
  finalSeq: number;
  metadata: Record<string, unknown>;
}

export interface SnapshotEvent {
  seq: number;
  event: unknown;
}

export interface SnapshotPayload {
  events: SnapshotEvent[];
}

export interface DeliveredPayload {
  from: number;
  to: number;
  kind: "chunk" | "snapshot";
  text: string;
}

export interface GenerationState {
  phase: GenerationPhase;
  generationId: string | null;
  idempotencyKey: string | null;
  text: string;
  cursor: number;
  deliveredPayloads: DeliveredPayload[];
  reconnectCount: number;
  duplicatesDropped: number;
  gapsSeen: number;
  lastCatchupMs: number | null;
  lastCatchupEvents: number | null;
  terminal: TerminalEvent | null;
  terminalMetadata: Record<string, unknown> | null;
  connectionEnabled: boolean;
  reconnectRequested: boolean;
  deferredTerminal: TerminalEvent | null;
  cancelPending: boolean;
  error: string | null;
}

export interface GenerationMessage {
  role: "system" | "user" | "assistant";
  content: string;
}

export interface StartGenerationInput {
  model?: string;
  messages: GenerationMessage[];
}

export interface ResumableStreamOptions {
  apiBaseUrl?: string;
  watchdogMs?: number;
  fetch?: typeof fetch;
  random?: () => number;
  now?: () => number;
}

const DEFAULT_API_BASE_URL = "http://localhost:8000";
const DEFAULT_WATCHDOG_MS = 30_000;
const RETRY_BASE_MS = 250;
const RETRY_CAP_MS = 5_000;
const MAX_DELIVERED_PAYLOADS = 24;

const terminalPhases = new Set<GenerationPhase>([
  "completed",
  "cancelled",
  "failed",
  "interrupted",
]);

export function createInitialGenerationState(
  connectionEnabled = true,
): GenerationState {
  return {
    phase: "idle",
    generationId: null,
    idempotencyKey: null,
    text: "",
    cursor: 0,
    deliveredPayloads: [],
    reconnectCount: 0,
    duplicatesDropped: 0,
    gapsSeen: 0,
    lastCatchupMs: null,
    lastCatchupEvents: null,
    terminal: null,
    terminalMetadata: null,
    connectionEnabled,
    reconnectRequested: false,
    deferredTerminal: null,
    cancelPending: false,
    error: null,
  };
}

export function isTerminalPhase(
  phase: GenerationPhase,
): phase is TerminalPhase {
  return terminalPhases.has(phase);
}

export function fullJitterDelay(
  retryIndex: number,
  random: () => number = Math.random,
): number {
  const ceiling = Math.min(
    RETRY_CAP_MS,
    RETRY_BASE_MS * 2 ** Math.max(0, retryIndex),
  );
  return Math.floor(Math.max(0, Math.min(1, random())) * ceiling);
}

export function textDeltaForEvent(event: unknown): string {
  if (typeof event === "string") {
    return event;
  }

  const record = asRecord(event);
  if (!record) {
    return "";
  }

  for (const key of ["text", "delta", "content", "token"] as const) {
    if (typeof record[key] === "string") {
      return record[key];
    }
  }

  const nestedDelta = asRecord(record.delta);
  if (nestedDelta) {
    for (const key of ["text", "content"] as const) {
      if (typeof nestedDelta[key] === "string") {
        return nestedDelta[key];
      }
    }
  }

  // Chunk envelopes vary between providers, so unwrap only known container fields.
  if (record.type === "chunk" || record.type === "text_delta") {
    return textDeltaForEvent(record.event ?? record.data);
  }

  return "";
}

export function applySequencedEvent(
  state: GenerationState,
  seq: number,
  event: unknown,
): GenerationState {
  if (!isPositiveInteger(seq)) {
    return isTerminalPhase(state.phase) ? state : markGap(state);
  }

  if (seq <= state.cursor) {
    return {
      ...state,
      duplicatesDropped: state.duplicatesDropped + 1,
    };
  }

  if (isTerminalPhase(state.phase)) {
    return state;
  }

  if (seq > state.cursor + 1) {
    return markGap(state);
  }

  const text = textDeltaForEvent(event);
  const next = {
    ...state,
    text: state.text + text,
    cursor: seq,
    deliveredPayloads: appendDeliveredPayload(state.deliveredPayloads, {
      from: seq,
      to: seq,
      kind: "chunk",
      text,
    }),
  };
  return applyDeferredTerminalIfReady(next);
}

export function applySnapshot(
  state: GenerationState,
  payload: SnapshotPayload,
): GenerationState {
  if (isTerminalPhase(state.phase)) {
    return state;
  }

  if (!payload || !Array.isArray(payload.events)) {
    return markGap(state);
  }

  let expectedSeq = state.cursor + 1;
  let duplicateCount = 0;
  let appendedText = "";

  // Validate the full suffix before appending any part of a snapshot.
  for (const rawEntry of payload.events as unknown[]) {
    const entry = asRecord(rawEntry);
    if (!entry || !isPositiveInteger(entry.seq) || !("event" in entry)) {
      return markGap({
        ...state,
        duplicatesDropped: state.duplicatesDropped + duplicateCount,
      });
    }

    if (entry.seq < expectedSeq) {
      duplicateCount += 1;
      continue;
    }

    if (entry.seq > expectedSeq) {
      return markGap({
        ...state,
        duplicatesDropped: state.duplicatesDropped + duplicateCount,
      });
    }

    appendedText += textDeltaForEvent(entry.event);
    expectedSeq += 1;
  }

  const cursor = expectedSeq - 1;
  if (cursor === state.cursor && duplicateCount === 0) {
    return state;
  }

  const next = {
    ...state,
    text: state.text + appendedText,
    cursor,
    duplicatesDropped: state.duplicatesDropped + duplicateCount,
    deliveredPayloads:
      cursor > state.cursor
        ? appendDeliveredPayload(state.deliveredPayloads, {
            from: state.cursor + 1,
            to: cursor,
            kind: "snapshot",
            text: appendedText,
          })
        : state.deliveredPayloads,
  };
  return applyDeferredTerminalIfReady(next);
}

function appendDeliveredPayload(
  payloads: DeliveredPayload[],
  payload: DeliveredPayload,
): DeliveredPayload[] {
  return [...payloads, payload].slice(-MAX_DELIVERED_PAYLOADS);
}

export function applyTerminalEvent(
  state: GenerationState,
  terminal: TerminalEvent,
): GenerationState {
  if (state.terminal || state.deferredTerminal) {
    return state;
  }

  if (
    !Number.isInteger(terminal.finalSeq) ||
    terminal.finalSeq < 0 ||
    !terminalPhases.has(terminal.type)
  ) {
    return markGap(state);
  }

  if (terminal.finalSeq > state.cursor) {
    // A terminal event cannot skip rows that are still missing locally.
    return {
      ...state,
      phase: state.generationId ? "reconnecting" : state.phase,
      reconnectRequested: true,
      deferredTerminal: terminal,
    };
  }

  return finalizeTerminal(state, terminal);
}

function applyDeferredTerminalIfReady(
  state: GenerationState,
): GenerationState {
  if (
    state.deferredTerminal &&
    state.cursor >= state.deferredTerminal.finalSeq
  ) {
    return finalizeTerminal(
      { ...state, deferredTerminal: null },
      state.deferredTerminal,
    );
  }

  return state;
}

function finalizeTerminal(
  state: GenerationState,
  terminal: TerminalEvent,
): GenerationState {
  return {
    ...state,
    phase: terminal.type,
    terminal,
    terminalMetadata: terminal.metadata,
    deferredTerminal: null,
    reconnectRequested: false,
    cancelPending: false,
    error:
      terminal.type === "failed"
        ? messageFromMetadata(terminal.metadata)
        : null,
  };
}

function markGap(state: GenerationState): GenerationState {
  return {
    ...state,
    phase: state.generationId ? "reconnecting" : state.phase,
    gapsSeen: state.gapsSeen + 1,
    reconnectRequested: true,
  };
}

function isPositiveInteger(value: unknown): value is number {
  return Number.isInteger(value) && Number(value) > 0;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : null;
}

function messageFromMetadata(metadata: Record<string, unknown>): string {
  for (const key of ["message", "error", "detail"] as const) {
    if (typeof metadata[key] === "string") {
      return metadata[key];
    }
  }
  return "Generation failed.";
}

function createIdempotencyKey(): string {
  const cryptoApi = globalThis.crypto;
  if (
    cryptoApi &&
    typeof cryptoApi.randomUUID === "function"
  ) {
    return cryptoApi.randomUUID();
  }

  const bytes = new Uint8Array(16);
  if (cryptoApi) {
    cryptoApi.getRandomValues(bytes);
  } else {
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0"));
  return [
    hex.slice(0, 4).join(""),
    hex.slice(4, 6).join(""),
    hex.slice(6, 8).join(""),
    hex.slice(8, 10).join(""),
    hex.slice(10).join(""),
  ].join("-");
}

interface FrameResult {
  cursorAdvanced: number;
  forceReconnect: boolean;
}

export class ResumableGeneration {
  private state: GenerationState;
  private readonly listeners = new Set<() => void>();
  private readonly controllers = new Set<AbortController>();
  private readonly readers =
    new Set<ReadableStreamDefaultReader<Uint8Array>>();
  private readonly gateWaiters = new Set<() => void>();
  private readonly retryWaiters = new Set<() => void>();
  private readonly stateWaiters = new Set<() => void>();
  private readonly apiBaseUrl: string;
  private readonly watchdogMs: number;
  private readonly fetchImpl: typeof fetch;
  private readonly random: () => number;
  private readonly now: () => number;
  private disposed = false;
  private lifecycle = 0;
  private streamRun = 0;
  private enableEpoch = 0;
  private startPromise: Promise<string> | null = null;
  private cancelWorker: Promise<void> | null = null;

  constructor(options: ResumableStreamOptions = {}) {
    this.apiBaseUrl = (
      options.apiBaseUrl ?? DEFAULT_API_BASE_URL
    ).replace(/\/+$/, "");
    this.watchdogMs = options.watchdogMs ?? DEFAULT_WATCHDOG_MS;
    this.fetchImpl = options.fetch ?? fetch.bind(globalThis);
    this.random = options.random ?? Math.random;
    this.now = options.now ?? (() => performance.now());
    this.state = createInitialGenerationState();
  }

  getState = (): GenerationState => this.state;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };

  async start(
    input: StartGenerationInput,
    requestedIdempotencyKey?: string,
  ): Promise<string> {
    this.assertUsable();

    if (this.state.generationId) {
      return this.state.generationId;
    }

    if (this.startPromise) {
      return this.startPromise;
    }

    const lifecycle = this.beginLifecycle();
    const idempotencyKey =
      requestedIdempotencyKey?.trim() || createIdempotencyKey();
    this.setState({
      ...createInitialGenerationState(this.state.connectionEnabled),
      phase: "starting",
      idempotencyKey,
    });

    const startPromise = this.startWithRetry(
      input,
      idempotencyKey,
      lifecycle,
    ).finally(() => {
      if (this.startPromise === startPromise) {
        this.startPromise = null;
      }
    });
    this.startPromise = startPromise;
    return startPromise;
  }

  attach(generationId: string, idempotencyKey: string | null = null): void {
    this.assertUsable();
    if (!generationId.trim()) {
      throw new Error("A generation ID is required.");
    }

    const lifecycle = this.beginLifecycle();
    this.setState({
      ...createInitialGenerationState(this.state.connectionEnabled),
      phase: this.state.connectionEnabled ? "starting" : "reconnecting",
      generationId,
      idempotencyKey,
    });
    this.launchEventLoop(generationId, lifecycle);
  }

  cancel(): void {
    this.assertUsable();
    if (this.state.phase === "idle" || isTerminalPhase(this.state.phase)) {
      return;
    }

    if (!this.state.cancelPending) {
      this.setState({ ...this.state, cancelPending: true });
    }

    if (!this.cancelWorker) {
      const lifecycle = this.lifecycle;
      const worker = this.runCancelWorker(lifecycle)
        .catch((error) => {
          if (
            this.isLifecycleCurrent(lifecycle) &&
            !isTerminalPhase(this.state.phase)
          ) {
            this.setState({
              ...this.state,
              error:
                error instanceof Error
                  ? error.message
                  : "Cancel request failed.",
            });
          }
        })
        .finally(() => {
          if (this.cancelWorker === worker) {
            this.cancelWorker = null;
          }
        });
      this.cancelWorker = worker;
    }
  }

  setConnectionEnabled(enabled: boolean): void {
    this.assertUsable();
    if (this.state.connectionEnabled === enabled) {
      return;
    }

    if (enabled) {
      this.enableEpoch += 1;
    }

    this.setState({
      ...this.state,
      connectionEnabled: enabled,
      phase:
        !enabled &&
        this.state.generationId &&
        !isTerminalPhase(this.state.phase)
          ? "reconnecting"
          : this.state.phase,
    });

    this.wake(this.retryWaiters);
    if (enabled) {
      this.wake(this.gateWaiters);
    } else {
      this.abortInFlight();
    }
  }

  clear(): void {
    this.assertUsable();
    this.beginLifecycle();
    this.setState(
      createInitialGenerationState(this.state.connectionEnabled),
    );
  }

  dispose(): void {
    if (this.disposed) {
      return;
    }

    this.disposed = true;
    this.lifecycle += 1;
    this.streamRun += 1;
    this.abortInFlight();
    this.wake(this.gateWaiters);
    this.wake(this.retryWaiters);
    this.wake(this.stateWaiters);
    this.listeners.clear();
  }

  private async startWithRetry(
    input: StartGenerationInput,
    idempotencyKey: string,
    lifecycle: number,
  ): Promise<string> {
    let retryIndex = 0;
    let observedEnableEpoch = this.enableEpoch;

    while (this.isLifecycleCurrent(lifecycle)) {
      await this.waitForConnection(lifecycle);
      this.assertLifecycle(lifecycle);
      if (!this.state.connectionEnabled) {
        continue;
      }
      observedEnableEpoch = this.enableEpoch;

      const controller = this.trackController();
      try {
        const response = await this.fetchImpl(
          `${this.apiBaseUrl}/generations`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              "Idempotency-Key": idempotencyKey,
            },
            body: JSON.stringify(input),
            signal: controller.signal,
          },
        );

        if (response.status !== 200 && response.status !== 201) {
          if (isRetryableStatus(response.status)) {
            throw new RetryableRequestError(
              `Start request returned ${response.status}.`,
            );
          }

          const detail = await response.text().catch(() => "");
          const message =
            detail || `Start request returned ${response.status}.`;
          this.setState({
            ...this.state,
            phase: "failed",
            error: message,
            terminalMetadata: {
              status: response.status,
              message,
            },
          });
          throw new Error(message);
        }

        const payload = (await response.json()) as {
          generation_id?: unknown;
        };
        if (
          typeof payload.generation_id !== "string" ||
          !payload.generation_id
        ) {
          const message = "Start response did not include generation_id.";
          this.setState({
            ...this.state,
            phase: "failed",
            error: message,
            terminalMetadata: { message },
          });
          throw new Error(message);
        }

        this.assertLifecycle(lifecycle);
        this.setState({
          ...this.state,
          generationId: payload.generation_id,
          error: null,
        });
        this.launchEventLoop(payload.generation_id, lifecycle);
        if (this.state.cancelPending) {
          this.cancel();
        }
        return payload.generation_id;
      } catch (error) {
        if (!this.isLifecycleCurrent(lifecycle)) {
          throw lifecycleEndedError();
        }

        if (
          error instanceof Error &&
          !(error instanceof RetryableRequestError) &&
          this.state.phase === "failed"
        ) {
          throw error;
        }

        if (!this.state.connectionEnabled) {
          continue;
        }

        observedEnableEpoch = await this.waitForRetry(
          retryIndex,
          observedEnableEpoch,
          lifecycle,
        );
        retryIndex += 1;
      } finally {
        this.untrackController(controller);
      }
    }

    throw lifecycleEndedError();
  }

  private launchEventLoop(generationId: string, lifecycle: number): void {
    const run = ++this.streamRun;
    void this.runEventLoop(generationId, lifecycle, run).catch((error) => {
      if (
        this.isStreamCurrent(lifecycle, run) &&
        !isTerminalPhase(this.state.phase)
      ) {
        this.setState({
          ...this.state,
          phase: "reconnecting",
          error: error instanceof Error ? error.message : "Stream failed.",
        });
      }
    });
  }

  private async runEventLoop(
    generationId: string,
    lifecycle: number,
    run: number,
  ): Promise<void> {
    let initialAttachment = true;
    let retryIndex = 0;
    let skipBackoff = false;
    let observedEnableEpoch = this.enableEpoch;

    while (
      this.isStreamCurrent(lifecycle, run) &&
      !isTerminalPhase(this.state.phase)
    ) {
      if (!initialAttachment && !skipBackoff) {
        observedEnableEpoch = await this.waitForRetry(
          retryIndex,
          observedEnableEpoch,
          lifecycle,
        );
        retryIndex += 1;
      }
      skipBackoff = false;

      if (
        !this.isStreamCurrent(lifecycle, run) ||
        isTerminalPhase(this.state.phase)
      ) {
        return;
      }

      await this.waitForConnection(lifecycle);
      if (
        !this.isStreamCurrent(lifecycle, run) ||
        isTerminalPhase(this.state.phase)
      ) {
        return;
      }
      if (!this.state.connectionEnabled) {
        skipBackoff = true;
        continue;
      }
      observedEnableEpoch = this.enableEpoch;

      const isReconnectAttempt = !initialAttachment;
      if (isReconnectAttempt) {
        this.setState({
          ...this.state,
          phase: "reconnecting",
          reconnectCount: this.state.reconnectCount + 1,
          reconnectRequested: false,
          error: null,
        });
      } else {
        this.setState({
          ...this.state,
          reconnectRequested: false,
          error: null,
        });
      }

      const catchupStartedAt = this.now();
      let catchupRecorded = false;
      let appliedInAttempt = false;
      let forceReconnect = false;
      let watchdogExpired = false;
      let watchdogTimer: ReturnType<typeof setTimeout> | undefined;
      const controller = this.trackController();
      let reader: ReadableStreamDefaultReader<Uint8Array> | null = null;

      const armWatchdog = () => {
        if (watchdogTimer !== undefined) {
          clearTimeout(watchdogTimer);
        }
        watchdogTimer = setTimeout(() => {
          watchdogExpired = true;
          controller.abort();
        }, this.watchdogMs);
      };

      try {
        armWatchdog();
        const response = await this.fetchImpl(
          `${this.apiBaseUrl}/generations/${encodeURIComponent(
            generationId,
          )}/events?after=${this.state.cursor}`,
          {
            headers: {
              Accept: "text/event-stream",
              "Cache-Control": "no-cache",
            },
            signal: controller.signal,
          },
        );

        if (!this.state.connectionEnabled) {
          throw new DOMException("Connection is disabled.", "AbortError");
        }

        if (response.status === 409) {
          this.setState({
            ...this.state,
            phase: "reconnecting",
            text: "",
            cursor: 0,
            deliveredPayloads: [],
            terminal: null,
            terminalMetadata: null,
            deferredTerminal: null,
            reconnectRequested: true,
            lastCatchupMs: null,
            lastCatchupEvents: null,
          });
          forceReconnect = true;
          skipBackoff = true;
          initialAttachment = false;
          continue;
        }

        if (response.status === 404) {
          this.failGoneGeneration();
          return;
        }

        if (!response.ok || !response.body) {
          throw new RetryableRequestError(
            `Event request returned ${response.status}.`,
          );
        }

        this.setState({
          ...this.state,
          phase: "attached",
          reconnectRequested: false,
          error: null,
        });

        reader = response.body.getReader();
        this.readers.add(reader);
        const parser = new SseParser();

        while (this.isStreamCurrent(lifecycle, run)) {
          const result = await reader.read();
          if (result.done) {
            const finalFrames = parser.finish();
            for (const frame of finalFrames) {
              const outcome = this.applyFrame(frame);
              if (outcome.cursorAdvanced > 0) {
                appliedInAttempt = true;
                if (isReconnectAttempt && !catchupRecorded) {
                  this.recordCatchup(
                    outcome.cursorAdvanced,
                    catchupStartedAt,
                  );
                  catchupRecorded = true;
                }
              }
              if (outcome.forceReconnect) {
                forceReconnect = true;
                skipBackoff = true;
                break;
              }
            }
            break;
          }

          if (result.value) {
            // Comment heartbeats produce no frames, but their bytes still
            // reset the inactivity watchdog.
            armWatchdog();
            const frames = parser.push(result.value);
            for (const frame of frames) {
              const outcome = this.applyFrame(frame);
              if (outcome.cursorAdvanced > 0) {
                appliedInAttempt = true;
                if (isReconnectAttempt && !catchupRecorded) {
                  this.recordCatchup(
                    outcome.cursorAdvanced,
                    catchupStartedAt,
                  );
                  catchupRecorded = true;
                }
              }

              if (
                outcome.forceReconnect ||
                isTerminalPhase(this.state.phase)
              ) {
                forceReconnect = outcome.forceReconnect;
                skipBackoff = outcome.forceReconnect;
                break;
              }
            }
          }

          if (forceReconnect || isTerminalPhase(this.state.phase)) {
            break;
          }
        }

        if (isTerminalPhase(this.state.phase)) {
          return;
        }

        this.setState({
          ...this.state,
          phase: "reconnecting",
          error: null,
        });
        initialAttachment = false;
        if (appliedInAttempt) {
          retryIndex = 0;
        }
        if (forceReconnect) {
          controller.abort();
        }
      } catch (error) {
        if (!this.isStreamCurrent(lifecycle, run)) {
          return;
        }

        if (isTerminalPhase(this.state.phase)) {
          return;
        }

        this.setState({
          ...this.state,
          phase: "reconnecting",
          error: null,
        });
        initialAttachment = false;

        if (!this.state.connectionEnabled) {
          skipBackoff = true;
        } else if (
          !watchdogExpired &&
          error instanceof DOMException &&
          error.name === "AbortError"
        ) {
          skipBackoff = true;
        }
      } finally {
        if (watchdogTimer !== undefined) {
          clearTimeout(watchdogTimer);
        }
        if (reader) {
          this.readers.delete(reader);
          void reader.cancel().catch(() => undefined);
        }
        this.untrackController(controller);
      }
    }
  }

  private applyFrame(frame: SseFrame): FrameResult {
    const previousCursor = this.state.cursor;
    const parsedData = parseJsonOrText(frame.data);
    let eventName = frame.event ?? "message";

    const dataRecord = asRecord(parsedData);
    if (
      eventName === "message" &&
      typeof dataRecord?.type === "string" &&
      ["chunk", "snapshot", "done", "cancelled", "failed", "interrupted"].includes(
        dataRecord.type,
      )
    ) {
      eventName = dataRecord.type;
    }

    if (eventName === "chunk") {
      const seq = parseSequence(frame.id ?? dataRecord?.seq);
      const event =
        dataRecord && "event" in dataRecord ? dataRecord.event : parsedData;
      this.setState(applySequencedEvent(this.state, seq, event));
    } else if (eventName === "snapshot") {
      const payload = asRecord(parsedData);
      const events = payload?.events;
      this.setState(
        applySnapshot(this.state, {
          events: Array.isArray(events)
            ? (events as SnapshotEvent[])
            : (events as never),
        }),
      );
    } else if (
      eventName === "done" ||
      eventName === "cancelled" ||
      eventName === "failed" ||
      eventName === "interrupted"
    ) {
      const terminal = parseTerminalEvent(eventName, parsedData, frame.id);
      if (terminal) {
        this.setState(applyTerminalEvent(this.state, terminal));
      } else {
        this.setState(markGap(this.state));
      }
    }

    return {
      cursorAdvanced: this.state.cursor - previousCursor,
      forceReconnect: this.state.reconnectRequested,
    };
  }

  private recordCatchup(
    eventCount: number,
    catchupStartedAt: number,
  ): void {
    this.setState({
      ...this.state,
      lastCatchupEvents: eventCount,
      lastCatchupMs: Math.max(0, Math.round(this.now() - catchupStartedAt)),
    });
  }

  private async runCancelWorker(lifecycle: number): Promise<void> {
    let retryIndex = 0;
    let observedEnableEpoch = this.enableEpoch;

    while (
      this.isLifecycleCurrent(lifecycle) &&
      this.state.cancelPending &&
      !isTerminalPhase(this.state.phase)
    ) {
      await this.waitForConnection(lifecycle);
      if (!this.isLifecycleCurrent(lifecycle)) {
        return;
      }
      if (!this.state.connectionEnabled) {
        continue;
      }
      observedEnableEpoch = this.enableEpoch;

      const generationId = this.state.generationId;
      if (!generationId) {
        await this.waitForStateChange(lifecycle);
        continue;
      }

      const controller = this.trackController();
      try {
        const response = await this.fetchImpl(
          `${this.apiBaseUrl}/generations/${encodeURIComponent(
            generationId,
          )}/cancel`,
          {
            method: "POST",
            signal: controller.signal,
          },
        );

        if (response.ok) {
          return;
        }
        throw new RetryableRequestError(
          `Cancel request returned ${response.status}.`,
        );
      } catch {
        if (
          !this.isLifecycleCurrent(lifecycle) ||
          isTerminalPhase(this.state.phase)
        ) {
          return;
        }

        if (!this.state.connectionEnabled) {
          continue;
        }

        observedEnableEpoch = await this.waitForRetry(
          retryIndex,
          observedEnableEpoch,
          lifecycle,
        );
        retryIndex += 1;
      } finally {
        this.untrackController(controller);
      }
    }
  }

  private async waitForConnection(lifecycle: number): Promise<void> {
    while (
      this.isLifecycleCurrent(lifecycle) &&
      !this.state.connectionEnabled
    ) {
      await new Promise<void>((resolve) => {
        this.gateWaiters.add(resolve);
      });
    }
    this.assertLifecycle(lifecycle);
  }

  private async waitForRetry(
    retryIndex: number,
    observedEnableEpoch: number,
    lifecycle: number,
  ): Promise<number> {
    if (observedEnableEpoch !== this.enableEpoch) {
      return this.enableEpoch;
    }

    const delay = fullJitterDelay(retryIndex, this.random);
    if (delay === 0) {
      return this.enableEpoch;
    }

    await new Promise<void>((resolve) => {
      let timer: ReturnType<typeof setTimeout>;
      const finish = () => {
        clearTimeout(timer);
        this.retryWaiters.delete(finish);
        resolve();
      };
      timer = setTimeout(finish, delay);
      this.retryWaiters.add(finish);
    });
    this.assertLifecycle(lifecycle);
    return this.enableEpoch;
  }

  private async waitForStateChange(lifecycle: number): Promise<void> {
    await new Promise<void>((resolve) => {
      this.stateWaiters.add(resolve);
    });
    this.assertLifecycle(lifecycle);
  }

  private beginLifecycle(): number {
    this.lifecycle += 1;
    this.streamRun += 1;
    this.startPromise = null;
    this.cancelWorker = null;
    this.abortInFlight();
    this.wake(this.gateWaiters);
    this.wake(this.retryWaiters);
    this.wake(this.stateWaiters);
    return this.lifecycle;
  }

  private trackController(): AbortController {
    const controller = new AbortController();
    this.controllers.add(controller);
    return controller;
  }

  private untrackController(controller: AbortController): void {
    this.controllers.delete(controller);
  }

  private abortInFlight(): void {
    for (const controller of this.controllers) {
      controller.abort();
    }
    this.controllers.clear();

    for (const reader of this.readers) {
      void reader.cancel().catch(() => undefined);
    }
    this.readers.clear();
  }

  private failGoneGeneration(): void {
    const message = "Generation was not found.";
    this.setState({
      ...this.state,
      phase: "failed",
      error: message,
      terminal: {
        type: "failed",
        finalSeq: this.state.cursor,
        metadata: { status: 404 },
      },
      terminalMetadata: {
        status: 404,
        message,
      },
    });
  }

  private setState(nextState: GenerationState): void {
    if (nextState === this.state) {
      return;
    }

    this.state = nextState;
    for (const listener of this.listeners) {
      listener();
    }
    this.wake(this.stateWaiters);
    if (isTerminalPhase(nextState.phase)) {
      this.wake(this.retryWaiters);
      this.wake(this.gateWaiters);
    }
  }

  private wake(waiters: Set<() => void>): void {
    for (const resolve of waiters) {
      resolve();
    }
    waiters.clear();
  }

  private isLifecycleCurrent(lifecycle: number): boolean {
    return !this.disposed && this.lifecycle === lifecycle;
  }

  private isStreamCurrent(lifecycle: number, run: number): boolean {
    return this.isLifecycleCurrent(lifecycle) && this.streamRun === run;
  }

  private assertLifecycle(lifecycle: number): void {
    if (!this.isLifecycleCurrent(lifecycle)) {
      throw lifecycleEndedError();
    }
  }

  private assertUsable(): void {
    if (this.disposed) {
      throw new Error("This transport has been disposed.");
    }
  }
}

class RetryableRequestError extends Error {}

function lifecycleEndedError(): DOMException {
  return new DOMException("The transport lifecycle ended.", "AbortError");
}

function isRetryableStatus(status: number): boolean {
  return status === 408 || status === 425 || status === 429 || status >= 500;
}

function parseJsonOrText(data: string): unknown {
  if (!data) {
    return {};
  }

  try {
    return JSON.parse(data) as unknown;
  } catch {
    return data;
  }
}

function parseSequence(value: unknown): number {
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string" && /^\d+$/.test(value)) {
    return Number(value);
  }
  return Number.NaN;
}

function parseTerminalEvent(
  eventName: "done" | "cancelled" | "failed" | "interrupted",
  value: unknown,
  frameId?: string,
): TerminalEvent | null {
  const record = asRecord(value) ?? {};
  const finalSeq = parseSequence(
    record.final_seq ?? record.finalSeq ?? frameId ?? 0,
  );
  if (!Number.isInteger(finalSeq) || finalSeq < 0) {
    return null;
  }

  const explicitMetadata = asRecord(record.metadata);
  const metadata = explicitMetadata
    ? { ...explicitMetadata }
    : { ...record };
  delete metadata.final_seq;
  delete metadata.finalSeq;
  delete metadata.type;

  return {
    type: eventName === "done" ? "completed" : eventName,
    finalSeq,
    metadata,
  };
}
