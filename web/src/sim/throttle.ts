// Port of conduit/core/ratelimit.py and conduit/core/breaker.py.
//
// TokenBucket paces one connector's sends and absorbs Retry-After pauses.
// CircuitBreaker pauses the connector entirely while its target looks down,
// so queued messages keep their receive count instead of burning it on an outage.

/** `rate` tokens per second up to `burst`; acquire() reserves one and returns the wait. */
export class TokenBucket {
  readonly capacity: number;
  tokens: number;
  notBefore: number;
  private updated: number;

  constructor(
    readonly rate: number,
    burst = 1,
    readonly maxPenalty = 60,
    private readonly clock: () => number = () => 0,
  ) {
    if (rate <= 0 || burst < 1) throw new Error("rate must be positive and burst at least 1");
    this.capacity = burst;
    this.tokens = burst;
    this.updated = clock();
    this.notBefore = this.updated;
  }

  private refill(): number {
    const now = this.clock();
    const elapsed = Math.max(0, now - this.updated);
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.rate);
    this.updated = now;
    return now;
  }

  /**
   * Reserve one token; return the seconds to wait before it is valid (0 when free).
   * Tokens go negative while reservations are outstanding, which is what keeps
   * consecutive sends 1/rate apart once the burst is spent.
   */
  acquire(): number {
    const now = this.refill();
    const wait = this.tokens >= 1 ? 0 : (1 - this.tokens) / this.rate;
    this.tokens -= 1;
    return Math.max(wait, this.notBefore - now, 0);
  }

  /** Pause every send on this connector for `seconds`, capped at maxPenalty. */
  penalize(seconds: number): number {
    const pause = Math.min(Math.max(0, seconds), this.maxPenalty);
    this.notBefore = Math.max(this.notBefore, this.clock() + pause);
    return pause;
  }

  available(): number {
    this.refill();
    return Math.max(0, this.tokens);
  }

  reset(): void {
    this.tokens = this.capacity;
    this.updated = this.clock();
    this.notBefore = this.updated;
  }
}

export type BreakerState = "closed" | "open" | "half_open";

export const STATE_VALUE: Record<BreakerState, number> = { closed: 0, half_open: 1, open: 2 };

/** Consecutive target failures open the circuit; one probe is admitted after recovery. */
export class CircuitBreaker {
  failures = 0;
  opens = 0;
  private openedAt: number | null = null;
  private probing = false;

  constructor(
    readonly failureThreshold = 5,
    readonly recoverySeconds = 30,
    private readonly clock: () => number = () => 0,
  ) {
    if (failureThreshold < 1 || recoverySeconds <= 0) {
      throw new Error("failureThreshold must be >= 1 and recoverySeconds > 0");
    }
  }

  get state(): BreakerState {
    if (this.openedAt === null) return "closed";
    return this.clock() - this.openedAt >= this.recoverySeconds ? "half_open" : "open";
  }

  /** Seconds until the breaker half-opens; 0 unless it is open. */
  remaining(): number {
    if (this.openedAt === null) return 0;
    return Math.max(0, this.recoverySeconds - (this.clock() - this.openedAt));
  }

  /** Whether a delivery may proceed now. Half-open admits exactly one probe. */
  allow(): boolean {
    const state = this.state;
    if (state === "closed") return true;
    if (state === "open" || this.probing) return false;
    this.probing = true;
    return true;
  }

  /** Reset; true when this closed a breaker that had been open. */
  recordSuccess(): boolean {
    const wasOpen = this.openedAt !== null;
    this.failures = 0;
    this.openedAt = null;
    this.probing = false;
    return wasOpen;
  }

  /** Count one target failure; true when this opened the breaker. */
  recordFailure(): boolean {
    this.failures += 1;
    const tripped = this.probing || this.failures >= this.failureThreshold;
    if (!tripped) return false;
    this.openedAt = this.clock();
    this.probing = false;
    this.failures = 0;
    this.opens += 1;
    return true;
  }

  reset(): void {
    this.failures = 0;
    this.opens = 0;
    this.openedAt = null;
    this.probing = false;
  }
}

/** 5xx, timeouts, and connection errors mean the target is unwell; 429 is throttling. */
export const isTargetFailure = (status: number | null) => status !== 429;
