import { useMemo, useState } from "react";
import { Code, Reveal, SectionHead } from "../components/common";
import { CONNECTOR_YAML, ConfigError, loadSpec, NEW_CONNECTOR_YAML } from "../sim/specs";
import { diffPlans, plan, type PlannedResource } from "../sim/terraform";
import "./ship.css";

function hcl(r: PlannedResource): string {
  const shortType = r.type;
  const name = r.address.split(".").pop()?.replace(/\["[^"]*"\]$/, "") ?? "this";
  const width = Math.max(...Object.keys(r.attributes).map((k) => k.length));
  const lines = Object.entries(r.attributes).map(([k, v]) => `      + ${k.padEnd(width)} = ${v.startsWith("(") ? v : JSON.stringify(v)}`);
  return `  # ${r.address} will be created\n  + resource "${shortType}" "${name}" {\n${lines.join("\n")}\n    }\n`;
}

export function Ship() {
  const [filename, setFilename] = useState("pager-oncall");
  const [yaml, setYaml] = useState(NEW_CONNECTOR_YAML);
  const [showExisting, setShowExisting] = useState(false);
  const before = useMemo(() => plan(CONNECTOR_YAML), []);
  const name = filename.trim().replace(/\.yaml$/, "");

  const result = useMemo(() => {
    try {
      const spec = loadSpec(name, yaml);
      const after = plan({ ...CONNECTOR_YAML, [name]: yaml });
      return { ok: true as const, spec, diff: diffPlans(before, after) };
    } catch (e) {
      return { ok: false as const, error: e instanceof ConfigError ? e.message : String(e) };
    }
  }, [name, yaml, before]);

  const collision = name in CONNECTOR_YAML;
  const lineCount = yaml.trimEnd().split("\n").length;

  return (
    <section className="section" id="ship" aria-labelledby="ship-title">
      <div className="wrap">
        <SectionHead eyebrow="04 / one file ships an integration" id="ship-title" title="Drop in a fourth YAML. Terraform plans exactly its resources." lede="terraform/main.tf runs fileset() over connectors/, yamldecode()s each file, and instantiates module.connector once per file. The module derives a queue and DLQ joined by a redrive policy, a least-privilege IAM policy, role, and attachment for the worker, and one SecureString SSM placeholder per declared secret. Edit the file; the plan follows." />
        <Reveal className="ship-grid" delay={0.1}>
          <div className="glass ship-editor">
            <div className="ship-file">
              <label htmlFor="ship-name" className="code-label">connectors/</label>
              <input id="ship-name" className="input" value={filename} onChange={(e) => setFilename(e.target.value.slice(0, 63))} aria-label="Connector file name without extension" />
              <span className="mono muted">.yaml · {lineCount} lines</span>
            </div>
            <textarea className="textarea" value={yaml} onChange={(e) => setYaml(e.target.value)} spellCheck={false} aria-label="Connector YAML" />
            <div className="ship-presets">
              <span className="code-label">try</span>
              <button className="btn btn-sm" onClick={() => { setFilename("pager-oncall"); setYaml(NEW_CONNECTOR_YAML); }}>pager-oncall (slack)</button>
              <button className="btn btn-sm" onClick={() => { setFilename("billing-jira"); setYaml(`type: jira\ntarget: BILL\nbase_url: https://billing.atlassian.net\nsecrets:\n  email: BILLING_JIRA_EMAIL\n  api_token: BILLING_JIRA_TOKEN\nqueue:\n  max_receive_count: 5\n  visibility_timeout_seconds: 90\n`); }}>billing-jira (two secrets, 8 resources)</button>
              <button className="btn btn-sm" onClick={() => { setFilename("audit-hook"); setYaml(`type: webhook\ntarget: https://audit.example.com/ingest\nsecrets:\n  signing_secret: AUDIT_HMAC_SECRET\nretry:\n  max_attempts: 3\n  base_seconds: 0.1\n  max_seconds: 0.05\n`); }}>audit-hook (invalid retry)</button>
            </div>
            {result.ok ? (
              <div className="ship-spec mono">
                <span><b>type</b> {result.spec.type}</span>
                <span><b>target</b> {result.spec.target}</span>
                <span><b>secrets</b> {Object.keys(result.spec.secrets).length}</span>
                <span><b>maxReceiveCount</b> {result.spec.queue.maxReceiveCount}</span>
                <span><b>visibility</b> {result.spec.queue.visibilityTimeoutSeconds}s</span>
                <span><b>max_attempts</b> {result.spec.retry.maxAttempts}</span>
              </div>
            ) : (
              <div className="ship-error mono" role="alert">
                <b>conduit config validate</b> connectors/{name || "?"}.yaml: {result.error}
              </div>
            )}
            {collision ? <div className="ship-error mono" role="alert">{name}.yaml already exists; the plan below shows changes to that connector instead of a new module instance.</div> : null}
          </div>

          <div className="ship-plan">
            <div className="glass ship-plan-head">
              <span className="mono muted">$ terraform -chdir=terraform plan -var-file=localstack.tfvars</span>
              {result.ok ? (
                <strong className={`mono ${result.diff.add.length === 7 && !collision ? "plan-seven" : ""}`}>{result.diff.summary}</strong>
              ) : (
                <strong className="mono plan-err">Error: invalid connector file</strong>
              )}
              {result.ok ? (
                <ul className="plan-list mono" aria-label="Resources to add">
                  {result.diff.add.map((r) => (
                    <li key={r.address} className="add">+ {r.address}</li>
                  ))}
                  {result.diff.change.map((r) => (
                    <li key={r.address} className="chg">~ {r.address}</li>
                  ))}
                </ul>
              ) : null}
            </div>
            {result.ok ? (
              <Code lang="hcl" label={`plan diff (${result.diff.add.length + result.diff.change.length} resources)`} className="ship-hcl" text={[...result.diff.add, ...result.diff.change].map(hcl).join("\n") || "No changes. Your infrastructure matches the configuration."} />
            ) : null}
            <button className="btn btn-sm" onClick={() => setShowExisting((s) => !s)} aria-expanded={showExisting}>
              {showExisting ? "Hide" : "Show"} the {before.resources.length} resources the three shipped connectors already create
            </button>
            {showExisting ? (
              <ul className="plan-list mono existing" aria-label="Existing resources">
                {before.resources.map((r) => <li key={r.address}>{r.address}</li>)}
              </ul>
            ) : null}
          </div>
        </Reveal>
      </div>
    </section>
  );
}
