import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type RefObject,
} from "react";
import {
  isTerminalPhase,
  type GenerationPhase,
} from "./resumableStream";
import { useGeneration } from "./useGeneration";

const DEFAULT_PROMPT =
  "Write about 1,200 words on the engineering challenges of streaming LLM tokens in production.";

const activePhases = new Set<GenerationPhase>([
  "starting",
  "attached",
  "reconnecting",
]);

export default function App() {
  const {
    state,
    start,
    clear,
    regenerate,
    setConnectionEnabled,
    restoredPrompt,
    isRestoring,
  } = useGeneration();
  const [prompt, setPrompt] = useState(DEFAULT_PROMPT);
  const [submittedPrompt, setSubmittedPrompt] = useState(restoredPrompt);
  const [requestError, setRequestError] = useState("");
  const [announcement, setAnnouncement] = useState("");
  const previousPhase = useRef(state.phase);
  const answerRef = useRef<HTMLDivElement>(null);
  const payloadLogRef = useRef<HTMLDivElement>(null);
  const lastCursorRef = useRef(0);
  const lastTextLenRef = useRef(0);
  const [payloads, setPayloads] = useState<
    { from: number; to: number; kind: "chunk" | "snapshot"; text: string }[]
  >([]);

  const debugVisible = useMemo(
    () => new URLSearchParams(window.location.search).get("debug") !== "0",
    [],
  );
  const displayPhase = isRestoring ? "starting" : state.phase;
  const isActive = activePhases.has(state.phase) || isRestoring;
  const canSubmit = (submittedPrompt || prompt).trim().length > 0 && !isActive;
  const connectionDisrupted =
    !state.connectionEnabled || displayPhase === "reconnecting";
  const connectionStatus = connectionDisrupted ? "disrupted" : "connected";
  const connectionStatusLabel =
    connectionStatus === "disrupted" ? "Disrupted" : "Connected";

  useEffect(() => {
    const previous = previousPhase.current;
    if (state.phase === "reconnecting" && previous !== "reconnecting") {
      setAnnouncement("Connection lost, reconnecting");
    } else if (state.phase === "attached" && previous === "reconnecting") {
      setAnnouncement("Reconnected");
    }
    previousPhase.current = state.phase;
  }, [state.phase]);

  useEffect(() => {
    if (state.cursor < lastCursorRef.current || state.phase === "idle") {
      setPayloads([]);
      lastCursorRef.current = state.cursor;
      lastTextLenRef.current = state.text.length;
      return;
    }

    if (state.cursor > lastCursorRef.current) {
      const from = lastCursorRef.current + 1;
      const to = state.cursor;
      const fragment = state.text.slice(lastTextLenRef.current);
      const kind: "chunk" | "snapshot" =
        to - from + 1 > 1 ? "snapshot" : "chunk";
      setPayloads((previous) =>
        [
          ...previous,
          {
            from,
            to,
            kind,
            text: fragment,
          },
        ].slice(-24),
      );
    }

    lastCursorRef.current = state.cursor;
    lastTextLenRef.current = state.text.length;
  }, [state.cursor, state.phase, state.text]);

  useEffect(() => {
    const node = answerRef.current;
    if (!node) {
      return;
    }
    node.scrollTop = node.scrollHeight;
  }, [state.text, displayPhase]);

  useEffect(() => {
    payloadLogRef.current?.scrollTo({ top: payloadLogRef.current.scrollHeight });
  }, [payloads]);

  async function handleGenerate() {
    const nextPrompt = (submittedPrompt || prompt).trim();
    if (!nextPrompt || isActive) {
      return;
    }

    setSubmittedPrompt(nextPrompt);
    setRequestError("");
    try {
      const input = {
        model: import.meta.env.VITE_MODEL || undefined,
        messages: [{ role: "user" as const, content: nextPrompt }],
      };
      if (state.generationId && isTerminalPhase(state.phase)) {
        await regenerate(input);
        return;
      }
      if (state.generationId) {
        clear();
      }
      await start(input);
    } catch (error) {
      setRequestError(
        error instanceof Error ? error.message : "Could not start generation.",
      );
    }
  }

  function handleStartAnother() {
    clear();
    setSubmittedPrompt("");
    setRequestError("");
  }

  async function handleRegenerate() {
    const nextPrompt = submittedPrompt || prompt.trim() || DEFAULT_PROMPT;
    setSubmittedPrompt(nextPrompt);
    setRequestError("");
    try {
      await regenerate({
        model: import.meta.env.VITE_MODEL || undefined,
        messages: [{ role: "user", content: nextPrompt }],
      });
    } catch (error) {
      setRequestError(
        error instanceof Error ? error.message : "Could not regenerate.",
      );
    }
  }

  return (
    <div className="app-shell">
      <main className="page">
        <section className="project-summary">
          <div className="project-title-row">
            <h1>Network-Resilient Token Streaming</h1>
            <span
              className={`title-status ${connectionStatus}`}
              role="status"
              aria-label={`Network ${connectionStatusLabel}`}
            >
              <StatusGlyph connected={connectionStatus === "connected"} />
              {connectionStatusLabel}
            </span>
            <span
              className={
                displayPhase === "attached"
                  ? "title-spinner spinning"
                  : "title-spinner"
              }
              role="status"
              aria-label={
                displayPhase === "attached" ? "Streaming" : undefined
              }
              aria-hidden={displayPhase !== "attached"}
            />
            <div className="summary-actions">
              {debugVisible && (
                <button
                  type="button"
                  className={
                    state.connectionEnabled
                      ? "top-connection-button connection-on"
                      : "top-connection-button connection-off"
                  }
                  onClick={() =>
                    setConnectionEnabled(!state.connectionEnabled)
                  }
                  aria-pressed={!state.connectionEnabled}
                >
                  <ConnectionIcon connected={state.connectionEnabled} />
                  {state.connectionEnabled
                    ? "Turn off connection"
                    : "Turn on connection"}
                </button>
              )}
            </div>
          </div>
          <p>
            Generation continues independently on the server; clients resume
            from a durable event log.
          </p>
          <div className="summary-rule" aria-hidden="true" />
        </section>

        <section className="stream-workspace" aria-label="Generation workspace">
          <div className="split-stage">
            <aside className="prompt-pane">
              <p className="turn-label">01 / PROMPT</p>
              <p className="prompt-copy">
                {submittedPrompt || prompt}
              </p>
              <button
                type="button"
                className="generate-button"
                onClick={() => void handleGenerate()}
                disabled={!canSubmit}
              >
                {isActive ? "Generating…" : "Generate answer"}
              </button>
              <PayloadDock
                payloads={payloads}
                logRef={payloadLogRef}
                debugVisible={debugVisible}
                cursor={state.cursor}
                reconnects={state.reconnectCount}
                duplicates={state.duplicatesDropped}
                gaps={state.gapsSeen}
                catchupEvents={state.lastCatchupEvents}
                catchupMs={state.lastCatchupMs}
              />
            </aside>

            <section className="answer-pane" aria-label="Streaming answer">
              <div className="answer-heading">
                <p className="turn-label">02 / RESPONSE</p>
                <div className="answer-heading-meta">
                  <PhaseLabel phase={displayPhase} />
                </div>
              </div>

              <div className="answer-scroll" ref={answerRef}>
                <AssistantBody
                  phase={displayPhase}
                  text={state.text}
                  error={requestError || state.error}
                />
                <TerminalActions
                  phase={state.phase}
                  onRegenerate={handleRegenerate}
                  onStartAnother={handleStartAnother}
                />
              </div>
            </section>
          </div>
        </section>
      </main>

      <div className="sr-only" aria-live="assertive" aria-atomic="true">
        {announcement}
      </div>
    </div>
  );
}

function PayloadDock({
  payloads,
  logRef,
  debugVisible,
  cursor,
  reconnects,
  duplicates,
  gaps,
  catchupEvents,
  catchupMs,
}: {
  payloads: PayloadTick[];
  logRef: RefObject<HTMLDivElement | null>;
  debugVisible: boolean;
  cursor: number;
  reconnects: number;
  duplicates: number;
  gaps: number;
  catchupEvents: number | null;
  catchupMs: number | null;
}) {
  return (
    <div className="payload-dock">
      <div className="payload-heading">
        <span>PAYLOADS</span>
      </div>
      <div className="payload-log" ref={logRef} aria-label="Delivered payloads">
        {payloads.length === 0 ? (
          <p className="payload-empty">Waiting for the first event</p>
        ) : (
          payloads.map((payload) => (
            <div key={`${payload.kind}-${payload.to}`} className="payload-row">
              <span className="payload-seq">
                {payload.kind === "snapshot"
                  ? `${payload.from}..${payload.to}`
                  : String(payload.to)}
              </span>
              <span className="payload-kind">{payload.kind}</span>
              <span className="payload-text">
                {formatPayloadText(payload.text)}
              </span>
            </div>
          ))
        )}
      </div>
      {debugVisible && (
        <dl className="payload-metrics">
          <div>
            <dt>cursor</dt>
            <dd>{cursor.toLocaleString()}</dd>
          </div>
          <div>
            <dt>reconnects</dt>
            <dd>{reconnects}</dd>
          </div>
          <div>
            <dt>duplicates</dt>
            <dd>{duplicates}</dd>
          </div>
          <div>
            <dt>gaps</dt>
            <dd>{gaps}</dd>
          </div>
          <div>
            <dt>catch-up</dt>
            <dd>
              {catchupEvents !== null && catchupMs !== null
                ? `${catchupEvents} / ${catchupMs}ms`
                : "n/a"}
            </dd>
          </div>
        </dl>
      )}
    </div>
  );
}

interface PayloadTick {
  from: number;
  to: number;
  kind: "chunk" | "snapshot";
  text: string;
}

function formatPayloadText(value: string): string {
  const compact = value.replace(/\s+/g, " ").trim();
  if (!compact) {
    return "·";
  }
  return compact.length > 72 ? `${compact.slice(0, 72)}…` : compact;
}

function AssistantBody({
  phase,
  text,
  error,
}: {
  phase: GenerationPhase;
  text: string;
  error: string | null;
}) {
  if (!text && phase === "idle") {
    return (
      <p className="empty-state">
        Generate an answer to start streaming.
      </p>
    );
  }

  if (!text && (phase === "starting" || phase === "attached")) {
    return (
      <div className="waiting-row">
        <span aria-hidden="true" />
        {phase === "starting"
          ? "Starting generation"
          : "Waiting for the first event"}
      </div>
    );
  }

  if (!text && phase === "reconnecting") {
    return (
      <p className="empty-state">
        The response will appear when the connection returns.
      </p>
    );
  }

  if (!text && phase === "failed") {
    return <p className="error-copy">{error || "Generation failed."}</p>;
  }

  return (
    <>
      <div className="response-text">
        {text}
        {phase === "attached" && (
          <span className="text-caret" aria-hidden="true" />
        )}
      </div>
      {error && phase === "failed" && (
        <p className="error-copy">{error}</p>
      )}
    </>
  );
}

function TerminalActions({
  phase,
  onRegenerate,
  onStartAnother,
}: {
  phase: GenerationPhase;
  onRegenerate: () => void;
  onStartAnother: () => void;
}) {
  if (!isTerminalPhase(phase)) {
    return null;
  }

  const labels = {
    completed: "Response complete",
    cancelled: "Generation stopped",
    failed: "Generation failed",
    interrupted: "Generation interrupted by a server restart",
  };

  return (
    <div className={`terminal-row terminal-${phase}`}>
      <span>{labels[phase]}</span>
      <div>
        {phase === "interrupted" && (
          <button type="button" onClick={onRegenerate}>
            Regenerate
          </button>
        )}
        <button type="button" onClick={onStartAnother}>
          New prompt
        </button>
      </div>
    </div>
  );
}

function PhaseLabel({ phase }: { phase: GenerationPhase }) {
  const labels: Record<GenerationPhase, string> = {
    idle: "READY",
    starting: "STARTING",
    attached: "STREAMING",
    reconnecting: "RECONNECTING",
    completed: "COMPLETE",
    cancelled: "STOPPED",
    failed: "FAILED",
    interrupted: "INTERRUPTED",
  };

  return (
    <span className={`phase-label phase-${phase}`}>
      <i aria-hidden="true" />
      {labels[phase]}
    </span>
  );
}

function StatusGlyph({ connected }: { connected: boolean }) {
  return (
    <svg viewBox="0 0 18 18" aria-hidden="true">
      {connected ? (
        <rect x="4.4" y="4.4" width="9.2" height="9.2" rx="1.2" />
      ) : (
        <>
          <path d="m4.2 4.2 9.6 9.6" />
          <path d="M13.8 4.2 4.2 13.8" />
        </>
      )}
    </svg>
  );
}

function ConnectionIcon({ connected }: { connected: boolean }) {
  return (
    <svg viewBox="0 0 20 16" aria-hidden="true">
      {connected ? (
        <>
          <path d="M2 5.5a12 12 0 0 1 16 0" />
          <path d="M5 9a7.5 7.5 0 0 1 10 0" />
          <path d="M8.2 12.2a2.8 2.8 0 0 1 3.6 0" />
          <circle cx="10" cy="14" r=".7" />
        </>
      ) : (
        <>
          <path d="M2 5.5a12 12 0 0 1 16 0" />
          <path d="M5 9a7.5 7.5 0 0 1 10 0" />
          <path d="m3 2 14 12" />
        </>
      )}
    </svg>
  );
}
