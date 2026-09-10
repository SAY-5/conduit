const REPO = "https://github.com/SAY-5/conduit";

export function Footer() {
  return (
    <footer className="footer">
      <div className="wrap footer-grid">
        <div>
          <span className="eyebrow">about this page</span>
          <p>
            A browser port of <a href={REPO} target="_blank" rel="noreferrer">SAY-5/conduit</a>, a Python 3.12 integration connector kit that runs against LocalStack (real SQS, DynamoDB, IAM, and SSM APIs) with Terraform 1.5 modules. The TypeScript in <code>web/src/sim</code> mirrors <code>conduit/core</code>, <code>conduit/adapters</code>, <code>conduit/worker.py</code>, the FastAPI fakes, and the resource set of <code>terraform/modules</code>; sha256 keys and HMAC signatures come from Web Crypto, time is a virtual clock, and randomness is a seeded PRNG so every run reproduces the README figures.
          </p>
        </div>
        <div>
          <span className="eyebrow">what is real, what is modelled</span>
          <ul className="footer-list">
            <li><b>Real:</b> key derivation, claim conditions, retry classification, jitter formula, redrive semantics, request shapes, the per-connector resource set.</li>
            <li><b>Modelled:</b> SQS and DynamoDB as in-memory tables, fake targets in-process, latencies as small seeded samples, Terraform as a resource derivation rather than the provider.</li>
            <li><b>Not on this page:</b> ECS task definitions, Prometheus scraping, structlog output, real network calls.</li>
          </ul>
        </div>
      </div>
      <nav className="wrap footer-links" aria-label="Source and documentation">
        <a href={REPO} target="_blank" rel="noreferrer">Repository</a>
        <a href={`${REPO}#readme`} target="_blank" rel="noreferrer">README</a>
        <a href={`${REPO}/blob/main/ARCHITECTURE.md`} target="_blank" rel="noreferrer">ARCHITECTURE.md</a>
        <a href={`${REPO}/blob/main/CONTRIBUTING.md`} target="_blank" rel="noreferrer">CONTRIBUTING.md</a>
        <a href={`${REPO}/tree/main/terraform`} target="_blank" rel="noreferrer">terraform/</a>
        <a href={`${REPO}/tree/main/terraform/modules`} target="_blank" rel="noreferrer">terraform/modules</a>
        <a href={`${REPO}/tree/main/connectors`} target="_blank" rel="noreferrer">connectors/</a>
        <a href={`${REPO}/blob/main/demo/run.py`} target="_blank" rel="noreferrer">demo/run.py</a>
        <a href={`${REPO}/tree/main/web/src/sim`} target="_blank" rel="noreferrer">web/src/sim</a>
      </nav>
      <div className="wrap footer-bottom mono">
        <span>conduit · MIT</span>
        <a href={REPO} target="_blank" rel="noreferrer">github.com/SAY-5/conduit</a>
      </div>
    </footer>
  );
}
