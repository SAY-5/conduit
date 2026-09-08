// Port of conduit/core/idempotency.py: key derivation and the conditional-put claim store.
//
// Key: sha256("v1|<connector>|<task id>|<task version>") via Web Crypto.
// Store: one table keyed by pk; claim() is a conditional PutItem with
//   attribute_not_exists(pk) OR (state = in_progress AND lease_until < now)

export const KEY_VERSION = "v1";

const encoder = new TextEncoder();

export async function sha256Hex(material: string): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", encoder.encode(material));
  return Array.from(new Uint8Array(digest), (b) => b.toString(16).padStart(2, "0")).join("");
}

export function keyMaterial(connector: string, taskId: string, taskVersion: number): string {
  return `${KEY_VERSION}|${connector}|${taskId}|${taskVersion}`;
}

export function idempotencyKey(connector: string, taskId: string, taskVersion: number): Promise<string> {
  return sha256Hex(keyMaterial(connector, taskId, taskVersion));
}

export const latestPk = (connector: string, taskId: string) => `latest|${connector}|${taskId}`;

export type ClaimState = "in_progress" | "delivered";

export interface ClaimResult {
  acquired: boolean;
  state: ClaimState | null;
  remoteId: string | null;
  /** Which branch of the ConditionExpression decided the outcome. */
  condition: "attribute_not_exists" | "expired_lease" | "failed";
}

export const isDuplicate = (c: ClaimResult) => !c.acquired && c.state === "delivered";

export interface StoreItem {
  pk: string;
  connector: string;
  taskId: string;
  state: ClaimState;
  leaseUntil: number;
  claimedAt: number;
  deliveredAt: number | null;
  expiresAt: number;
  remoteId: string | null;
}

export interface StoreEvent {
  op: "PutItem" | "UpdateItem" | "DeleteItem";
  pk: string;
  condition: string;
  outcome: "ok" | "ConditionalCheckFailedException";
}

/** MemoryIdempotencyStore with the DynamoDB store's semantics and a visible item table. */
export class IdempotencyStore {
  readonly items = new Map<string, StoreItem>();
  private readonly latestMap = new Map<string, string>();
  readonly events: StoreEvent[] = [];

  constructor(
    private readonly clock: () => number,
    readonly leaseSeconds = 120,
    readonly ttlSeconds = 7 * 24 * 3600,
  ) {}

  claim(key: string, connector: string, taskId: string): ClaimResult {
    const now = this.clock();
    const existing = this.items.get(key);
    const condition = "attribute_not_exists(pk) OR (#s = :inprog AND lease_until < :now)";
    if (existing && !(existing.state === "in_progress" && existing.leaseUntil < now)) {
      this.events.push({ op: "PutItem", pk: key, condition, outcome: "ConditionalCheckFailedException" });
      return { acquired: false, state: existing.state, remoteId: existing.remoteId, condition: "failed" };
    }
    const branch = existing ? "expired_lease" : "attribute_not_exists";
    this.items.set(key, {
      pk: key,
      connector,
      taskId,
      state: "in_progress",
      leaseUntil: now + this.leaseSeconds,
      claimedAt: now,
      deliveredAt: null,
      expiresAt: now + this.ttlSeconds,
      remoteId: null,
    });
    this.events.push({ op: "PutItem", pk: key, condition, outcome: "ok" });
    return { acquired: true, state: "in_progress", remoteId: null, condition: branch };
  }

  markDelivered(key: string, remoteId: string | null): void {
    const item = this.items.get(key);
    if (!item) return;
    item.state = "delivered";
    item.remoteId = remoteId;
    item.deliveredAt = this.clock();
    this.events.push({ op: "UpdateItem", pk: key, condition: "SET #s = :done, delivered_at = :now", outcome: "ok" });
    if (remoteId) this.latestMap.set(latestPk(item.connector, item.taskId), remoteId);
  }

  /** Drop an in-progress claim so a redrive or replay can deliver. Delivered records are never released. */
  release(key: string): void {
    const item = this.items.get(key);
    if (item?.state === "in_progress") {
      this.items.delete(key);
      this.events.push({ op: "DeleteItem", pk: key, condition: "#s = :inprog", outcome: "ok" });
    } else {
      this.events.push({ op: "DeleteItem", pk: key, condition: "#s = :inprog", outcome: "ConditionalCheckFailedException" });
    }
  }

  latest(connector: string, taskId: string): string | null {
    return this.latestMap.get(latestPk(connector, taskId)) ?? null;
  }

  get(key: string): StoreItem | undefined {
    return this.items.get(key);
  }

  reset(): void {
    this.items.clear();
    this.latestMap.clear();
    this.events.length = 0;
  }
}
