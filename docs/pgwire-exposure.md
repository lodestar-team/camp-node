# Exposing the Postgres-wire endpoint publicly

Status: **design / decision needed**. As of 2026-06-09 the read-only Postgres-wire
endpoint (`--pg-server`) ships in the binary and runs in prod **bound to `127.0.0.1:1705`** —
reachable on the box and via SSH tunnel, but **not** exposed at the `engine.camp` edge.

This doc lays out what it would take to make it publicly reachable, and recommends an approach.
The decision is yours: public pgwire changes our network posture (a new public TCP port), so it is
deliberately not flipped on by default.

## Why it isn't just a flag

`engine.camp` is an **HTTP** surface: Vercel (HTTPS) → the node shim (`:1606`) → ampd Flight
(`:1702`). That whole path translates REST/JSON to Flight/JSON-Lines. The Postgres wire protocol is
a **raw, stateful TCP protocol** — it cannot be proxied by Vercel or the HTTP shim. Public pgwire
therefore means terminating a new public TCP listener somewhere we control, with its own TLS and its
own abuse controls. None of the existing edge machinery applies.

## What the endpoint already gives us (and doesn't)

- **Read-only.** Plan visitors reject DDL/DML; only `SELECT`/introspection run. ✔
- **Catalog isolation.** Per-dataset resolution; one bad dataset can't blank the catalog. ✔ (now
  covered by `it_refresh.rs`).
- **Auth today:** the `AuthManager` is constructed empty. In practice connections succeed as user
  `postgres` and are refused for other usernames — this is *not* a real authentication boundary, it
  is an artifact of `datafusion-postgres`. Treat the endpoint as **unauthenticated** for exposure
  purposes.
- **No statement timeout / row cap / connection cap** is enforced at the pg layer today. A public
  read-only SQL port is a resource-exhaustion surface (a `SELECT` that scans every dataset, thousands
  of idle connections, etc.).

So the work for public exposure is overwhelmingly about **transport (TLS)**, **abuse control
(timeouts / caps / rate limits)**, and **blast radius**, not about correctness of the query path.

## Threat model (read-only, public, no-key — same spirit as the REST API)

camp is intentionally a free, no-key public API, so "anonymous access" is a feature, not a
vulnerability. The realistic risks are availability/cost, not data integrity:

1. **CPU/memory exhaustion** — an expensive query (cross-dataset scan, huge sort) ties up the node.
2. **Connection exhaustion** — many idle/slow connections starve the listener.
3. **Information exposure** — only what we already expose over REST/Flight (public chain data +
   catalog metadata). No new secrets. ✔ low concern.
4. **Amplification / unmetered egress** — large result sets pulled repeatedly.

## Options

### A. Localhost + SSH tunnel only (status quo — shipped)
Bind `127.0.0.1:1705`; users with box access (or an SSH tunnel) connect. Zero public surface.
- ✅ Already live, zero new risk, lets us dogfood BI tools (Grafana/Metabase/DBeaver) against prod.
- ❌ Not actually public; doesn't deliver the "connect psql to engine.camp" story.

### B. Public TCP via a sidecar TLS-terminating proxy (recommended for public)
Put a small TCP proxy in front (e.g. `ghostunnel`, `stunnel`, or HAProxy in TCP mode) that:
- terminates **TLS** (so `psql "sslmode=require"` works; clients never hit cleartext),
- enforces a **max concurrent connections** cap and a **per-IP connection rate limit**,
- forwards to `127.0.0.1:1705`.
Pair with ampd-side **statement timeout** + **result-row cap** (see "Hardening" below).
- ✅ Real public access; TLS without touching the Rust TLS stack; caps live in the proxy where
  they're easy to tune; ampd stays localhost-only (proxy is the only thing public).
- ✅ Reuses ops we already trust (systemd `--user` unit, like the shim).
- ❌ One more process to run and monitor; proxy and ampd must agree on limits.

### C. Native TLS + auth in `datafusion-postgres`
Configure the library's own TLS + a real `AuthManager` (issue a shared read-only credential, or
per-user tokens).
- ✅ One process; "proper" auth if we want named users.
- ❌ Couples us harder to `datafusion-postgres` (already version-pinned to DF50 — see
  `project_camp_node_pgwire`); TLS cert plumbing in-process; less mature than a battle-tested proxy.
  Higher risk for little gain given the data is public anyway.

### D. Don't expose; document the tunnel
Keep B's value (BI dogfooding) without public surface. Publish a one-liner SSH-tunnel recipe for
power users.
- ✅ Zero risk. ❌ Not public.

## Recommendation

**Ship B (sidecar TLS proxy) when we want public pgwire**, gated on first doing the ampd-side
hardening below. Until then, **A is the correct resting state** (it's what's live). B keeps ampd
localhost-only, terminates TLS in a mature proxy, and puts connection/rate caps exactly where they
belong — mirroring how the HTTP edge already fronts Flight. C buys named-user auth we don't need for
public chain data while deepening a pin we want to keep shallow.

## Hardening to land before any public exposure (B or C)

These are endpoint-level, independent of transport, and should exist before `:1705` faces the
internet:

1. **Statement timeout** — cap query wall-clock (e.g. 30s) so no single query pins the node.
2. **Result-row / result-byte cap** — bound egress per query.
3. **Max concurrent connections** + **idle-connection timeout** — bound the connection table.
4. **Per-IP connection rate limit** — at the proxy (B) or via firewall (`nft`/Vercel WAF analog).
5. **Memory pool** — confirm the pg path honours the same `query_max_mem_mb` the Flight path uses
   (the serving `SessionContext` should be built from the same `QueryEnv`; verify, don't assume).
6. **Observability** — connection count + slow-query logging on `:1705`, so abuse is visible.

Items 1–2 and 5 are ampd-side code; 3–4 can live in the proxy. Track them as the public-exposure
checklist.

## Operational notes

- Prod unit: `~/.config/systemd/user/ampd-fork.service`, `ExecStart … dev --pg-server 127.0.0.1:1705`.
- Connect (on box / tunnel): `psql "host=127.0.0.1 port=1705 user=postgres dbname=amp sslmode=disable"`.
- Schemas are **version-qualified**: `"_/arbitrum_one@4.0.1"."blocks"` (not `"_/arbitrum_one"."blocks"`).
- Rollback: drop `--pg-server` from the unit, `daemon-reload`, restart. (The CLI fix in
  `13b9be8` ensures `--pg-server` is additive and never disables Flight/JSON-Lines/Admin.)
