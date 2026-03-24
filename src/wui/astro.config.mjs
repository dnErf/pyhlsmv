// @ts-check
import { defineConfig } from 'astro/config';

import vue from '@astrojs/vue';

import cloudflare from '@astrojs/cloudflare';

import tailwindcss from '@tailwindcss/vite';

// https://astro.build/config
export default defineConfig({
  integrations: [vue()],
  adapter: cloudflare(),
  output: "server",

  vite: {
    plugins: [tailwindcss()]
  }
});