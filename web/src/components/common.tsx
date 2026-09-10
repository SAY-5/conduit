import { motion, useReducedMotion } from "framer-motion";
import { useEffect, useRef, useState, type ReactNode } from "react";

export function Reveal({ children, delay = 0, className }: { children: ReactNode; delay?: number; className?: string }) {
  const reduced = useReducedMotion();
  return (
    <motion.div
      className={className}
      initial={reduced ? false : { opacity: 0, y: 22 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true, margin: "-60px" }}
      transition={{ duration: 0.7, delay, ease: [0.22, 1, 0.36, 1] }}
    >
      {children}
    </motion.div>
  );
}

export function SectionHead({ eyebrow, title, lede, id }: { eyebrow: string; title: ReactNode; lede?: ReactNode; id?: string }) {
  return (
    <Reveal>
      <span className="eyebrow">{eyebrow}</span>
      <h2 className="section-title" id={id}>
        {title}
      </h2>
      {lede ? <p className="section-lede">{lede}</p> : null}
    </Reveal>
  );
}

/** Tweens a number toward its target; jumps when reduced motion is preferred. */
export function Counter({ value, digits = 0, suffix = "", className }: { value: number; digits?: number; suffix?: string; className?: string }) {
  const reduced = useReducedMotion();
  const [shown, setShown] = useState(value);
  // Where the tween is right now, so a target that changes mid-flight keeps the
  // number continuous instead of restarting from the last settled value.
  const current = useRef(value);
  useEffect(() => {
    if (reduced) {
      current.current = value;
      setShown(value);
      return;
    }
    const start = current.current;
    const target = value;
    if (start === target) {
      setShown(target);
      return;
    }
    let raf = 0;
    let t0: number | null = null;
    const dur = 450;
    const step = (ts: number) => {
      if (t0 === null) t0 = ts;
      const p = Math.min(1, (ts - t0) / dur);
      const eased = 1 - (1 - p) ** 3;
      const v = p < 1 ? start + (target - start) * eased : target;
      current.current = v;
      setShown(v);
      if (p < 1) raf = requestAnimationFrame(step);
    };
    raf = requestAnimationFrame(step);
    return () => cancelAnimationFrame(raf);
  }, [value, reduced]);
  return (
    <span className={className}>
      {shown.toFixed(digits)}
      {suffix}
    </span>
  );
}

type Lang = "yaml" | "json" | "hcl" | "plain" | "py";

function highlight(text: string, lang: Lang): ReactNode[] {
  const lines = text.split("\n");
  return lines.map((line, i) => {
    let nodes: ReactNode;
    if (lang === "yaml") {
      const m = line.match(/^(\s*)([^:#]+)(:)(.*)$/);
      const comment = line.match(/^(\s*)(#.*)$/);
      if (comment) nodes = <span className="c">{line}</span>;
      else if (m) {
        const [, indent, key, colon, rest] = m;
        const hash = rest.indexOf(" #");
        const value = hash >= 0 ? rest.slice(0, hash) : rest;
        const trailing = hash >= 0 ? rest.slice(hash) : "";
        const isNum = /^\s*-?\d/.test(value);
        nodes = (
          <>
            {indent}
            <span className="k">{key}</span>
            {colon}
            <span className={isNum ? "n" : "s"}>{value}</span>
            {trailing ? <span className="c">{trailing}</span> : null}
          </>
        );
      } else nodes = line;
    } else if (lang === "json") {
      const parts = line.split(/("(?:[^"\\]|\\.)*")(\s*:)?/g);
      nodes = parts.map((p, j) => {
        if (p === undefined) return null;
        if (/^".*"$/.test(p)) return <span key={j} className={parts[j + 1] === ":" ? "k" : "s"}>{p}</span>;
        if (/^-?\d+(\.\d+)?$/.test(p.trim())) return <span key={j} className="n">{p}</span>;
        return <span key={j}>{p}</span>;
      });
    } else if (lang === "hcl") {
      if (/^\s*\+/.test(line)) nodes = <span className="add">{line}</span>;
      else if (/^\s*-/.test(line)) nodes = <span className="del">{line}</span>;
      else if (/^\s*#/.test(line)) nodes = <span className="c">{line}</span>;
      else nodes = line;
    } else if (lang === "py") {
      const parts = line.split(/(\b(?:def|class|return|raise|if|else|for|in|not|and|or|None|True|False|self)\b|"[^"]*"|#.*$)/g);
      nodes = parts.map((p, j) => {
        if (!p) return null;
        if (/^#/.test(p)) return <span key={j} className="c">{p}</span>;
        if (/^"/.test(p)) return <span key={j} className="s">{p}</span>;
        if (/^(def|class|return|raise|if|else|for|in|not|and|or|None|True|False|self)$/.test(p)) return <span key={j} className="k">{p}</span>;
        return <span key={j}>{p}</span>;
      });
    } else nodes = line;
    return (
      <span key={i} className="code-line">
        {nodes}
        {"\n"}
      </span>
    );
  });
}

export function Code({ text, lang = "plain", className = "", label }: { text: string; lang?: Lang; className?: string; label?: string }) {
  return (
    <div className={`code-wrap ${className}`}>
      {label ? <span className="code-label">{label}</span> : null}
      <pre className="code" tabIndex={0}>
        <code>{highlight(text, lang)}</code>
      </pre>
    </div>
  );
}

export function Chip({ tone = "", children }: { tone?: string; children: ReactNode }) {
  return <span className={`chip ${tone}`}>{children}</span>;
}

export const TYPE_TONE: Record<string, string> = { slack: "chip-slack", jira: "chip-jira", webhook: "chip-webhook" };
export const TYPE_COLOR: Record<string, string> = { slack: "var(--slack)", jira: "var(--jira)", webhook: "var(--hook)" };
