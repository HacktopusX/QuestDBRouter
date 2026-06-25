/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_STREAM_WS?: string;
  readonly VITE_WS_PROXY_TARGET?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
