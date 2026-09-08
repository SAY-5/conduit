import { Hero } from "./sections/Hero";
import { Interface } from "./sections/Interface";
import { Reliability } from "./sections/Reliability";
import { Dlq } from "./sections/Dlq";
import { Ship } from "./sections/Ship";
import { Footer } from "./components/Footer";

const REPO = "https://github.com/SAY-5/conduit";

function Nav() {
  return (
    <div className="wrap">
      <nav className="nav glass" aria-label="Page sections">
        <a className="nav-brand" href="#top" aria-label="Conduit, back to top">
          <svg viewBox="0 0 32 32" aria-hidden>
            <rect width="32" height="32" rx="7" fill="#0b1210" />
            <path d="M6 16h7l3-6 3 12 3-6h4" fill="none" stroke="#19c2a0" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          conduit
        </a>
        <div className="nav-links">
          <a href="#interface">Interface</a>
          <a href="#reliability">Idempotency + retries</a>
          <a href="#dlq">DLQ + replay</a>
          <a href="#ship">Ship one file</a>
        </div>
        <a className="btn btn-sm nav-repo" href={REPO} target="_blank" rel="noreferrer">
          SAY-5/conduit
        </a>
      </nav>
    </div>
  );
}

export default function App() {
  return (
    <div id="top">
      <a className="sr-only" href="#interface">Skip to content</a>
      <Nav />
      <main>
        <Hero />
        <Interface />
        <Reliability />
        <Dlq />
        <Ship />
      </main>
      <Footer />
    </div>
  );
}
