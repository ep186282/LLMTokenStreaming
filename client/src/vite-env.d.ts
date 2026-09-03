/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_BASE_URL?: string;
  readonly VITE_MODEL?: string;
  readonly VITE_RECONNECT_PILL_MS?: string;
  readonly VITE_WATCHDOG_S?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
