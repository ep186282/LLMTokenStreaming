import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  useSyncExternalStore,
} from "react";
import {
  ResumableGeneration,
  type StartGenerationInput,
} from "./resumableStream";

const STORAGE_KEY = "resumable-generation.identity.v1";

interface PersistedGeneration {
  generationId: string;
  idempotencyKey: string;
  prompt?: string;
}

interface PersistedStart {
  idempotencyKey: string;
  input: StartGenerationInput;
}

type PersistedRecord = PersistedGeneration | PersistedStart;

export function useGeneration() {
  const restoredRecord = useMemo(readPersistedRecord, []);
  const [restoreAttempted, setRestoreAttempted] = useState(!restoredRecord);
  const transport = useMemo(
    () =>
      new ResumableGeneration({
        apiBaseUrl:
          import.meta.env.VITE_API_BASE_URL || "http://localhost:8000",
        watchdogMs: secondsToMilliseconds(
          import.meta.env.VITE_WATCHDOG_S,
          30,
        ),
      }),
    [],
  );
  const state = useSyncExternalStore(
    transport.subscribe,
    transport.getState,
    transport.getState,
  );

  useEffect(() => {
    if (restoredRecord) {
      restoreRecord(transport, restoredRecord);
    }
    setRestoreAttempted(true);

    return () => {
      transport.dispose();
    };
  }, [restoredRecord, transport]);

  useEffect(() => {
    if (
      state.phase === "failed" &&
      state.terminalMetadata?.status === 404
    ) {
      removePersistedGeneration();
      return;
    }

    if (state.generationId && state.idempotencyKey) {
      persistGenerationIdentity(
        state.generationId,
        state.idempotencyKey,
      );
    }
  }, [
    state.generationId,
    state.idempotencyKey,
    state.phase,
    state.terminalMetadata,
  ]);

  useEffect(() => {
    const handleStorage = (event: StorageEvent) => {
      if (
        event.key !== STORAGE_KEY ||
        !event.newValue ||
        transport.getState().phase !== "idle"
      ) {
        return;
      }

      const record = parsePersistedRecord(event.newValue);
      if (record) {
        restoreRecord(transport, record);
      }
    };

    window.addEventListener("storage", handleStorage);
    return () => {
      window.removeEventListener("storage", handleStorage);
    };
  }, [transport]);

  const start = useCallback(
    async (input: StartGenerationInput) => {
      const started = transport.start(input);
      const nextState = transport.getState();
      if (nextState.idempotencyKey) {
        persistRecord({
          input,
          idempotencyKey: nextState.idempotencyKey,
        });
      }

      let generationId: string;
      try {
        generationId = await started;
      } catch (error) {
        if (!isLifecycleAbort(error)) {
          removePersistedGeneration();
        }
        throw error;
      }
      const acceptedState = transport.getState();
      if (acceptedState.idempotencyKey) {
        persistGenerationIdentity(
          generationId,
          acceptedState.idempotencyKey,
          promptFromInput(input),
        );
      }
      return generationId;
    },
    [transport],
  );

  const clear = useCallback(() => {
    removePersistedGeneration();
    transport.clear();
  }, [transport]);

  const regenerate = useCallback(
    async (input: StartGenerationInput) => {
      removePersistedGeneration();
      transport.clear();
      return start(input);
    },
    [start, transport],
  );

  const stop = useCallback(() => {
    transport.cancel();
  }, [transport]);

  const setConnectionEnabled = useCallback(
    (enabled: boolean) => {
      transport.setConnectionEnabled(enabled);
    },
    [transport],
  );

  return {
    state,
    start,
    stop,
    clear,
    regenerate,
    setConnectionEnabled,
    restoredPrompt: promptFromRecord(restoredRecord),
    isRestoring:
      Boolean(restoredRecord) && !restoreAttempted && state.phase === "idle",
  };
}

function readPersistedRecord(): PersistedRecord | null {
  try {
    const value = window.localStorage.getItem(STORAGE_KEY);
    if (!value) {
      return null;
    }

    const parsed = parsePersistedRecord(value);
    if (parsed) {
      return parsed;
    }

    window.localStorage.removeItem(STORAGE_KEY);
  } catch {
    // Storage can be unavailable in privacy-restricted contexts.
  }
  return null;
}

function parsePersistedRecord(
  value: string,
): PersistedRecord | null {
  try {
    const parsed = asRecord(JSON.parse(value));
    if (!parsed || typeof parsed.idempotencyKey !== "string") {
      return null;
    }

    if (
      typeof parsed.generationId === "string" &&
      parsed.generationId &&
      parsed.idempotencyKey
    ) {
      return {
        generationId: parsed.generationId,
        idempotencyKey: parsed.idempotencyKey,
        ...(typeof parsed.prompt === "string" && parsed.prompt
          ? { prompt: parsed.prompt }
          : {}),
      };
    }

    const input = parseStartInput(parsed.input);
    if (input && parsed.idempotencyKey) {
      return {
        idempotencyKey: parsed.idempotencyKey,
        input,
      };
    }
  } catch {
    return null;
  }
  return null;
}

function persistRecord(value: PersistedRecord): void {
  try {
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(value));
  } catch {
    // Streaming still works when identity persistence is unavailable.
  }
}

function persistGenerationIdentity(
  generationId: string,
  idempotencyKey: string,
  prompt?: string,
) {
  const existing = readPersistedRecord();
  const keptPrompt =
    prompt ||
    (existing && "prompt" in existing ? existing.prompt : "") ||
    (existing && "input" in existing ? promptFromInput(existing.input) : "") ||
    "";
  persistRecord({
    generationId,
    idempotencyKey,
    ...(keptPrompt ? { prompt: keptPrompt } : {}),
  });
}

function promptFromInput(input: StartGenerationInput): string {
  for (let index = input.messages.length - 1; index >= 0; index -= 1) {
    if (input.messages[index].role === "user") {
      return input.messages[index].content;
    }
  }
  return "";
}

function promptFromRecord(record: PersistedRecord | null): string {
  if (!record) {
    return "";
  }
  if ("input" in record) {
    return promptFromInput(record.input);
  }
  return record.prompt || "";
}

function restoreRecord(
  transport: ResumableGeneration,
  record: PersistedRecord,
): void {
  if ("generationId" in record) {
    // Text is rebuilt from zero because only generation identity is retained.
    transport.attach(record.generationId, record.idempotencyKey);
    return;
  }

  void transport
    .start(record.input, record.idempotencyKey)
    .then((generationId) => {
      persistGenerationIdentity(
        generationId,
        record.idempotencyKey,
        promptFromInput(record.input),
      );
    })
    .catch((error) => {
      if (!isLifecycleAbort(error)) {
        removePersistedGeneration();
      }
    });
}

function parseStartInput(value: unknown): StartGenerationInput | null {
  const record = asRecord(value);
  if (!record || !Array.isArray(record.messages)) {
    return null;
  }

  const messages = record.messages.filter((message) => {
    const item = asRecord(message);
    return (
      item !== null &&
      ["system", "user", "assistant"].includes(String(item.role)) &&
      typeof item.content === "string"
    );
  });
  if (messages.length !== record.messages.length || messages.length === 0) {
    return null;
  }
  if (record.model !== undefined && typeof record.model !== "string") {
    return null;
  }

  return {
    ...(typeof record.model === "string" ? { model: record.model } : {}),
    messages: messages as StartGenerationInput["messages"],
  };
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : null;
}

function isLifecycleAbort(error: unknown): boolean {
  return error instanceof DOMException && error.name === "AbortError";
}

function removePersistedGeneration(): void {
  try {
    window.localStorage.removeItem(STORAGE_KEY);
  } catch {
    // Clearing in-memory state remains useful when storage is unavailable.
  }
}

function secondsToMilliseconds(value: string | undefined, fallback: number) {
  const seconds = Number(value ?? fallback);
  return Number.isFinite(seconds) && seconds > 0
    ? seconds * 1_000
    : fallback * 1_000;
}
