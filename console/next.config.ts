import type { NextConfig } from "next";

// `next build` writes a static export (out/) that the aether binary embeds and
// serves on the admin listener. `next dev` instead proxies the admin API of a
// local server, so the console can be developed with hot reloading.
const dev = process.env.NODE_ENV === "development";
const adminApi = process.env.AETHER_ADMIN_URL ?? "http://127.0.0.1:9001";

const config: NextConfig = {
  output: dev ? undefined : "export",
  trailingSlash: true,
  images: { unoptimized: true },
  reactStrictMode: true,
  poweredByHeader: false,
  ...(dev && {
    async rewrites() {
      return [{ source: "/api/:path*", destination: `${adminApi}/api/:path*` }];
    },
  }),
};

export default config;
