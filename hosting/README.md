# Hosting kit — link.phantasma.info (Phantasma Link v5 host)

The relay container (this repo's `docker-compose.yml`) plus a static universal-link host
behind nginx, forming the public `link.phantasma.info`:

    Cloudflare (proxied) -> nginx (TLS, vhost) -> relay container on 127.0.0.1:7200

## Contents

- `nginx/link.phantasma.info.conf` — the vhost: static files + AASA content-type + `/v5`
  fallback + `/relay` WebSocket proxy + `/healthz`. Origin restricted to Cloudflare IPs
  (`cloudflare-allow.conf` + `deny all`); TLS via the shared `*.phantasma.info` origin cert.
- `site/` — the static files served from the vhost `root`:
  - `.well-known/assetlinks.json` — Android App Links verification.
  - `.well-known/apple-app-site-association` — iOS Universal Links verification (no extension).
  - `v5/index.html` — install-the-wallet fallback for `/v5/*` (preserves the URL fragment).
  - `index.html` — bare landing page.

## Deploy

1. Static: copy `site/` to `/var/www/link-phantasma-info/site` (root-owned, dirs 755 / files 644).
2. Relay: `docker compose up -d --build` (binds `127.0.0.1:7200`). If the docker daemon reports
   `all predefined address pools have been fully subnetted` (host networks exhausted), add an
   UNTRACKED `docker-compose.override.yml` so the container joins the existing default bridge
   instead of creating a new one:

       services:
         relay:
           network_mode: bridge

3. nginx: install `nginx/link.phantasma.info.conf` into the server's `sites-available/` and symlink
   it into `sites-enabled/` (the TLS lines already point at the shared `cf-phantasma.info` origin
   cert), then `nginx -t && systemctl reload nginx`.

## Verify (through the public chain)

    curl -si https://link.phantasma.info/healthz                                 # 200 "ok"
    curl -si https://link.phantasma.info/.well-known/assetlinks.json             # 200 application/json
    curl -si https://link.phantasma.info/.well-known/apple-app-site-association  # 200 application/json
    curl -si https://link.phantasma.info/v5/pair                                 # 200 fallback HTML
    curl --http1.1 -si https://link.phantasma.info/relay \
      -H 'Connection: Upgrade' -H 'Upgrade: websocket' \
      -H 'Sec-WebSocket-Version: 13' -H 'Sec-WebSocket-Key: aSBhbSBhIHRlc3Qga2V5IQ=='  # 101 Switching Protocols
