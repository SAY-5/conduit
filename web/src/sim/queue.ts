// Port of conduit/core/queue.py against an in-memory SQS: visibility timeout,
// ApproximateReceiveCount, redrive into the DLQ after maxReceiveCount, list, replay.

import type { Envelope } from "./models";

export interface QueueMessage {
  messageId: string;
  envelope: Envelope;
  receiveCount: number;
  visibleAt: number;
  sentAt: number;
  /** How many times this body was moved from the DLQ back to the work queue. */
  replays: number;
  /** Set when SQS moved the message into a DLQ. */
  deadLetteredAt: number | null;
}

export interface RedriveEvent {
  messageId: string;
  taskId: string;
  receiveCount: number;
  at: number;
}

export class Queue {
  readonly messages: QueueMessage[] = [];
  readonly redrives: RedriveEvent[] = [];
  private seq = 0;
  dlq: Queue | null = null;
  maxReceiveCount = 3;
  received = 0;
  sent = 0;
  deleted = 0;

  constructor(
    readonly name: string,
    readonly clock: () => number,
    readonly visibilityTimeoutSeconds: number,
    readonly idPrefix: string,
  ) {}

  configureRedrive(dlq: Queue, maxReceiveCount: number): void {
    this.dlq = dlq;
    this.maxReceiveCount = maxReceiveCount;
  }

  redrivePolicy(): { deadLetterTargetArn: string; maxReceiveCount: number } | null {
    return this.dlq ? { deadLetterTargetArn: arn(this.dlq.name), maxReceiveCount: this.maxReceiveCount } : null;
  }

  send(envelope: Envelope, delaySeconds = 0, replays = 0): string {
    const now = this.clock();
    const messageId = `${this.idPrefix}-${(++this.seq).toString(36).padStart(4, "0")}`;
    this.messages.push({ messageId, envelope, receiveCount: 0, visibleAt: now + delaySeconds, sentAt: now, replays, deadLetteredAt: null });
    this.sent += 1;
    return messageId;
  }

  sendBatch(envelopes: Envelope[]): number {
    for (const env of envelopes) this.send(env);
    return envelopes.length;
  }

  /** Receive up to max visible messages. A message past maxReceiveCount is moved to the DLQ by the queue, not the worker. */
  receive(max = 10, visibility?: number): QueueMessage[] {
    const now = this.clock();
    const out: QueueMessage[] = [];
    for (const m of this.messages.slice()) {
      if (out.length >= max) break;
      if (m.visibleAt > now) continue;
      if (this.dlq && m.receiveCount >= this.maxReceiveCount) {
        this.moveToDlq(m);
        continue;
      }
      m.receiveCount += 1;
      m.visibleAt = now + (visibility ?? this.visibilityTimeoutSeconds);
      this.received += 1;
      out.push(m);
    }
    return out;
  }

  private moveToDlq(m: QueueMessage): void {
    const idx = this.messages.indexOf(m);
    if (idx >= 0) this.messages.splice(idx, 1);
    const now = this.clock();
    this.redrives.push({ messageId: m.messageId, taskId: m.envelope.task.id, receiveCount: m.receiveCount, at: now });
    this.dlq!.messages.push({ ...m, visibleAt: now, deadLetteredAt: now });
    this.dlq!.sent += 1;
  }

  changeVisibility(messageId: string, seconds: number): void {
    const m = this.messages.find((x) => x.messageId === messageId);
    if (m) m.visibleAt = this.clock() + Math.max(0, Math.min(43200, seconds));
  }

  delete(messageId: string): void {
    const idx = this.messages.findIndex((x) => x.messageId === messageId);
    if (idx >= 0) {
      this.messages.splice(idx, 1);
      this.deleted += 1;
    }
  }

  depth(): { visible: number; notVisible: number; total: number } {
    const now = this.clock();
    let visible = 0;
    for (const m of this.messages) if (m.visibleAt <= now) visible += 1;
    return { visible, notVisible: this.messages.length - visible, total: this.messages.length };
  }

  purge(): void {
    this.messages.length = 0;
    this.redrives.length = 0;
  }
}

export const arn = (name: string) => `arn:aws:sqs:us-east-1:000000000000:${name}`;

/** Peek at dead letters without consuming them; visibility is handed back immediately. */
export function listDeadLetters(dlq: Queue, limit?: number): QueueMessage[] {
  const batch = dlq.receive(limit ?? 1000, 30);
  for (const m of batch) dlq.changeVisibility(m.messageId, 0);
  return batch;
}

/** Move dead letters back onto target with attempt + 1 and a fresh receive count. */
export function replayDeadLetters(dlq: Queue, target: Queue, limit?: number): QueueMessage[] {
  const moved: QueueMessage[] = [];
  for (const m of dlq.receive(limit ?? 1000, 60)) {
    const envelope: Envelope = { ...m.envelope, attempt: m.envelope.attempt + 1 };
    target.send(envelope, 0, m.replays + 1);
    dlq.delete(m.messageId);
    moved.push(m);
  }
  return moved;
}
