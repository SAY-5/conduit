# conduit/web

Static browser demo of the connector kit. React 18 and TypeScript in strict mode, bundled by
Vite; no backend, no Docker, nothing to install beyond npm.

`src/sim/` is a port of the Python package, not a mock of it: `prng.ts` (seeded PRNG and a
virtual clock), `models.ts`, `specs.ts` (the YAML subset and `ConnectorSpec` validation),
`idempotency.ts` (sha256 keys via Web Crypto and the conditional-put claim store), `queue.ts`
(visibility timeout, receive count, redrive, list and replay), `retry.ts` (classification and
full-jitter backoff), `throttle.ts` (token bucket and circuit breaker), `adapters.ts` (the three
adapters and the fake targets), `schema.ts` (the shipped source schemas; a payload that fails
one goes to the quarantine queue), `worker.ts`, `terraform.ts` (the resource set the modules
create), and `engine.ts` (the queues, workers, and the `make demo` scenario). Nothing in
`src/sim` calls `Math.random`, `Date.now`, or `eval`, so a given seed always produces the same
run.

## Commands

```
npm install
npm run dev         # vite dev server
npm run build       # tsc -b && vite build, into dist/
npm run selfcheck   # 44 assertions in node, exits non-zero on failure
```

`npm run selfcheck` reproduces the figures in the top-level README (300 submitted, 60
deduplicated, 60 retries, 10 dead-lettered and replayed to 0, 8 resources planned for a fourth
connector YAML, 26 for the shipped three) and adds unit assertions on key derivation, the claim conditions, the backoff
ceiling, the token bucket, the breaker, the redrive threshold, and a malformed payload landing in
quarantine rather than the DLQ.

## Sections

| id | what it shows |
| --- | --- |
| hero | the whole scenario running as a fan-out board with live counters |
| `#interface` | one `deliver()` contract, the parsed spec, and the rendered request per connector |
| `#reliability` | idempotency claims, the 429 backoff timeline, and a hard 400 bouncing into the DLQ |
| `#dlq` | dead letters, a malformed payload held in quarantine, clearing the fault, replaying, draining to zero |
| `#ship` | an editable connector YAML and the Terraform plan diff it produces |
| `#run` | `make demo` end to end: live counters, worker log, and the summary block |

## Deployment

`vercel.json` builds with `npm run build` and serves `dist/` as a single-page app. The page is
static: it fetches nothing at runtime beyond its own bundle and two font stylesheets.
