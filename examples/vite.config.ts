import {defineConfig} from 'vite';
import react from '@vitejs/plugin-react-swc';
import * as path from 'node:path';
import {fileURLToPath} from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const sdkRoot = path.resolve(here, '..');

/**
 * Vite config for the example browser client.
 *
 * Resolves `@foony/realtime` to the SDK's TypeScript source (`../src`) rather than the published
 * tarball, so the example always exercises the latest local code with no rebuild step. The dev
 * server is granted fs access to the SDK root so it can serve that source.
 */
export default defineConfig({
  root: path.resolve(here, 'client'),
  resolve: {
    alias: [
      {find: /^@foony\/realtime$/, replacement: path.resolve(sdkRoot, 'src/index.ts')},
    ],
  },
  server: {
    port: 5180,
    fs: {allow: [sdkRoot]},
  },
  plugins: [react()],
});
