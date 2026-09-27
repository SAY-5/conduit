# conduit/web

Static browser demo of the connector kit. React 18 and TypeScript in strict mode, bundled by
Vite; no backend, no Docker, nothing to install beyond npm.

`connectors/*.yaml`, `schemas/*/v*.yaml` and the `Adapter` class the page quotes are embedded
verbatim into `src/sim/config.generated.ts` by `scripts/embed-config.mjs`; `npm run embed:check`
fails when the generated module and those files disagree, and CI runs it, so the configuration the
page parses is the configuration the worker parses.

`src/sim/` is a port of the Python package, not a mock of it: `prng.ts` (seeded PRNG and a
virtual clock), `models.ts`, `specs.ts` (the YAML subset and `ConnectorSpec` validation),
`idempotency.ts` (sha256 keys via Web Crypto and the conditional-put claim store), `queue.ts`
(visibility timeout, receive count, redrive, list and replay), `retry.ts` (classification and
full-jitter backoff), `throttle.ts` (token bucket and circuit breaker), `adapters.ts` (the three
adapters and the fake targets), `schema.ts` (the shipped source schemas, parsed from the embedded YAML; a
payload that fails one goes to the quarantine queue), `mapping.ts` (render, default, coerce,
validate and truncate every remote field, as `conduit/core/mapping.py` does), `worker.ts`, `terraform.ts` (the resource set the modules
create), and `engine.ts` (the queues, workers, and the `make demo` scenario). Nothing in
`src/sim` calls `Math.random`, `Date.now`, or `eval`, so a given seed always produces the same
run.

## Commands

```
npm install
npm run dev         # vite dev server
npm run build       # tsc -b && vite build, into dist/
npm run embed       # regenerate src/sim/config.generated.ts from connectors/ and schemas/
npm run embed:check # fail when the generated module no longer matches those files
npm run selfcheck   # 48 assertions in node, exits non-zero on failure
```

`npm run selfcheck` reproduces the figures in the top-level README (302 submitted, 2 quarantined, 60
deduplicated, 60 retries, 10 dead-lettered and replayed to 0, 8 resources planned for a fourth
connector YAML, 26 for the shipped three) and adds unit assertions on key derivation, the claim conditions, the backoff
ceiling, the token bucket, the breaker, the redrive threshold, the values parsed out of the shipped
YAMLs (jira's burst of 5, its 5/30s breaker, its 120s Retry-After cap, the 255-character summary
rule), and both quarantine stages: a payload that fails its source schema and a title that fails
the webhook connector's `max_length` mapping rule, each landing in quarantine rather than the DLQ.

## Sections

| id | what it shows |
| --- | --- |
| hero | the whole scenario running as a fan-out board with live counters |
| `#interface` | one `deliver()` contract, the parsed spec, and the rendered request per connector |
| `#reliability` | idempotency claims, the 429 backoff timeline, and a hard 400 bouncing into the DLQ |
| `#dlq` | dead letters, a malformed payload held in quarantine, clearing the fault, replaying, draining to zero |
| `#ship` | an editable connector YAML and the Terraform plan diff it produces |
| `#run` | `make demo`'s delivery half: live counters, worker log, and the summary block without the ops and cost tables |

## Deployment

`vercel.json` builds with `npm run build` and serves `dist/` as a single-page app. The page is
static: it fetches nothing at runtime beyond its own bundle and two font stylesheets.

`public/preview.png` is the Open Graph card: a 1200x630 headless-Chrome screenshot of this page
mid-run, not an illustration of it. The card's meta tags carry no absolute URL because the
deployment host is not recorded in the repository, so `og:image` resolves against whichever
origin serves the page.

## Accessibility

The worker log and the counter grid are not live regions: they change every few tens of
milliseconds, which an assistive technology would read as an unbroken stream. Each section
announces its phase transitions and its settled outcome through a single `role="status"`
element instead, the log is a focusable scrollable region, and the particle legend is readable
text with `aria-hidden` on the colour swatches alone.
