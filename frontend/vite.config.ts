import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const wsProxyTarget = env.VITE_WS_PROXY_TARGET ?? "ws://localhost:8080";

  return {
    server: {
      port: 5173,
      host: true,
      proxy: {
        "/ws": {
          target: wsProxyTarget,
          ws: true,
        },
      },
    },
  };
});
