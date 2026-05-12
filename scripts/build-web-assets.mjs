import { build } from "esbuild";

await build({
  entryPoints: ["web-src/sql-editor.js"],
  bundle: true,
  format: "iife",
  platform: "browser",
  target: ["es2020"],
  outfile: "internal/web/assets/sql-editor.bundle.js",
  minify: false,
  sourcemap: false,
  logLevel: "info",
  banner: {
    js: "window.Log2SQL = window.Log2SQL || {};",
  },
});
